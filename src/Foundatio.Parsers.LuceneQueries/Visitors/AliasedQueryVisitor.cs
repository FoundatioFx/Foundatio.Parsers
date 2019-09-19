using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class AliasedQueryVisitor : ChainableQueryVisitor {
        private readonly bool _useNestedResolvers;

        public AliasedQueryVisitor(bool useNestedResolvers = true) {
            _useNestedResolvers = useNestedResolvers;
        }

        public override Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            ApplyAlias(node, context);

            return base.VisitAsync(node, context);
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            ApplyAlias(node, context);
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            ApplyAlias(node, context);
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            ApplyAlias(node, context);
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            ApplyAlias(node, context);
        }

        private void ApplyAlias(IFieldQueryNode node, IQueryVisitorContext context) {
            if (node.Parent == null)
                return;

            var resolver = node.Parent.GetAliasResolver(context);
            var result = resolver != null && node.Field != null ? resolver(node.Field) : null;
            if (result == null) {
                if (node is GroupNode groupNode)
                    node.SetAliasResolver(_useNestedResolvers ? GetScopedResolver(resolver, node.Field) : resolver);

                return;
            }

            node.SetOriginalField(node.Field);
            node.Field = result.Name;
            if (node is GroupNode)
                node.SetAliasResolver(_useNestedResolvers ? result.Resolver : resolver);
        }

        public static AliasResolver GetScopedResolver(AliasResolver resolver, string scope) {
            return f => resolver(!String.IsNullOrEmpty(scope) ? scope + "." + f : f);
        }

        public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            var rootResolver = context.GetRootAliasResolver();
            if (rootResolver != null) {
                if (node is GroupNode)
                    node.SetAliasResolver(rootResolver);
                else
                    throw new InvalidOperationException("Node must be GroupNode.");
            }

            await node.AcceptAsync(this, context).ConfigureAwait(false);

            return node;
        }

        public static Task<IQueryNode> RunAsync(IQueryNode node, AliasResolver resolver, IQueryVisitorContextWithAliasResolver context = null) {
            return new AliasedQueryVisitor().AcceptAsync(node, context ?? new QueryVisitorContextWithAliasResolver { RootAliasResolver = resolver });
        }

        public static IQueryNode Run(IQueryNode node, AliasResolver resolver, IQueryVisitorContextWithAliasResolver context = null) {
            return RunAsync(node, resolver, context).GetAwaiter().GetResult();
        }

        public static Task<IQueryNode> RunAsync(IQueryNode node, AliasMap aliasMap, IQueryVisitorContextWithAliasResolver context = null) {
            return new AliasedQueryVisitor().AcceptAsync(node, context ?? new QueryVisitorContextWithAliasResolver { RootAliasResolver = aliasMap.Resolve });
        }

        public static IQueryNode Run(IQueryNode node, AliasMap aliasMap, IQueryVisitorContextWithAliasResolver context = null) {
            return RunAsync(node, aliasMap, context).GetAwaiter().GetResult();
        }
    }

    public delegate GetAliasResult AliasResolver(string field);
    public delegate Task<string> IncludeResolver(string name);
    
    [DebuggerDisplay("{Name}")]
    public class GetAliasResult {
        public string Name { get; set; }
        public AliasResolver Resolver { get; set; }
    }

    public class AliasMap : Dictionary<string, AliasMapValue> {
        public void Add(string key, string value) {
            Add(key, new AliasMapValue { Name = value });
        }

        public GetAliasResult Resolve(string field) {
            if (String.IsNullOrEmpty(field))
                return null;

            var currentResolver = InternalResolve(this);
            var result = InternalResolve(field, currentResolver);
            if (result != null)
                return result;

            string[] fieldParts = field.Split('.');
            for (int i = 0; i < fieldParts.Length; i++) {
                var currentResult = InternalResolve(fieldParts, i, currentResolver);
                if (currentResult == null)
                    continue;

                fieldParts = fieldParts.Skip(i + 1).ToArray();
                i = -1;
                currentResult.Name = (result != null ? result.Name + "." : String.Empty) + currentResult.Name;
                result = currentResult;
                currentResolver = result.Resolver;
            }

            if (result == null)
                return null;

            if (fieldParts.Length > 0)
                result.Name = result.Name + "." + String.Join(".", fieldParts);

            return result;
        }

        private AliasResolver InternalResolve(AliasMap aliasMap) {
            return field => {
                if (aliasMap.TryGetValue(field, out var aliasMapValue))
                    return new GetAliasResult {
                        Name = aliasMapValue.Name,
                        Resolver = f => aliasMapValue.HasChildMappings ? aliasMapValue.ChildMap.Resolve(f) : null
                    };

                return null;
            };
        }

        private GetAliasResult InternalResolve(string field, AliasResolver resolver) {
            var result = resolver?.Invoke(field);
            if (result != null) {
                return new GetAliasResult {
                    Name = result.Name,
                    Resolver = result.Resolver // need to wrap this
                };
            }

            return null;
        }

        private GetAliasResult InternalResolve(string[] fieldParts, int index, AliasResolver resolver) {
            string part = index == 0 ? fieldParts[0] : String.Join(".", fieldParts.Take(index + 1));
            return InternalResolve(part, resolver);
        }
    }

    [DebuggerDisplay("{Name} HasChildMappings: {HasChildMappings}")]
    public class AliasMapValue {
        public string Name { get; set; }

        public bool HasChildMappings => _aliasMap != null && _aliasMap.Count > 0;

        private AliasMap _aliasMap;
        public AliasMap ChildMap => _aliasMap ?? (_aliasMap = new AliasMap());
    }
}

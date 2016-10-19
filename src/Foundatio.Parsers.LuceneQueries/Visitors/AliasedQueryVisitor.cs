using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class AliasedQueryVisitor : ChainableQueryVisitor {
        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            ApplyAlias(node, context);

            foreach (var child in node.Children)
                child.Accept(this, context);
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

            var resolver = node.Parent.GetAliasResolver();
            var result = resolver(node.Field);
            if (result == null) {
                var groupNode = node as GroupNode;
                if (groupNode != null)
                    node.SetAliasResolver(GetScopedResolver(resolver, node.Field));

                return;
            }

            node.Field = result.Name;
            if (node is GroupNode)
                node.SetAliasResolver(result.Resolver);
        }

        public static AliasResolver GetScopedResolver(AliasResolver resolver, string scope) {
            return f => resolver(!String.IsNullOrEmpty(scope) ? scope + "." + f : f);
        }

        public override IQueryNode Accept(IQueryNode node, IQueryVisitorContext context) {
            var aliasResolverContext = context as IQueryVisitorContextWithAliasResolver;
            var rootResolver = aliasResolverContext?.RootAliasResolver;
            if (rootResolver == null)
                throw new ArgumentNullException(nameof(context), "Context must have a root alias resolver set.");

            if (node is GroupNode)
                node.SetAliasResolver(rootResolver);
            else
                throw new InvalidOperationException("Node must be GroupNode.");

            node.Accept(this, context);

            return node;
        }

        public static IQueryNode Run(GroupNode node, AliasResolver resolver, IQueryVisitorContextWithAliasResolver context = null) {
            return new AliasedQueryVisitor().Accept(node, context ?? new QueryVisitorContextWithAliasResolver { RootAliasResolver = resolver });
        }

        public static IQueryNode Run(GroupNode node, AliasMap aliasMap, IQueryVisitorContextWithAliasResolver context = null) {
            return new AliasedQueryVisitor().Accept(node, context ?? new QueryVisitorContextWithAliasResolver { RootAliasResolver = aliasMap.Resolve });
        }
    }

    public delegate GetAliasResult AliasResolver(string field);

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

            var fieldParts = field.Split('.');
            var currentResolver = InternalResolve(this);
            GetAliasResult result = null;
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
                AliasMapValue aliasMapValue;
                if (aliasMap.TryGetValue(field, out aliasMapValue))
                    return new GetAliasResult {
                        Name = aliasMapValue.Name,
                        Resolver = f => aliasMapValue.HasChildMappings ? aliasMapValue.ChildMap.Resolve(f) : null
                    };

                return null;
            };
        }

        private GetAliasResult InternalResolve(string[] fieldParts, int index, AliasResolver resolver) {
            var part = index == 0 ? fieldParts[0] : String.Join(".", fieldParts.Take(index + 1));
            var result = resolver?.Invoke(part);
            if (result != null) {
                return new GetAliasResult {
                    Name = result.Name,
                    Resolver = result.Resolver // need to wrap this
                };
            }

            return null;
        }
    }

    public class AliasMapValue {
        public string Name { get; set; }

        public bool HasChildMappings => _aliasMap != null && _aliasMap.Count > 0;

        private AliasMap _aliasMap;
        public AliasMap ChildMap => _aliasMap ?? (_aliasMap = new AliasMap());
    }
}

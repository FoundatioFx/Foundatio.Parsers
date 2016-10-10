using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class AliasedQueryVisitor : ChainableQueryVisitor {
        private readonly AliasResolver _rootResolver;

        public AliasedQueryVisitor(AliasResolver aliasResolver) {
            if (aliasResolver == null)
                throw new ArgumentNullException(nameof(aliasResolver));

            _rootResolver = aliasResolver;
        }

        public AliasedQueryVisitor(AliasMap aliasMap) {
            if (aliasMap == null)
                throw new ArgumentNullException(nameof(aliasMap));

            _rootResolver = aliasMap.Resolve;
        }

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
            if (node is GroupNode)
                node.SetAliasResolver(_rootResolver);

            node.Accept(this, context);
            return node;
        }

        public static IQueryNode Run(GroupNode node, AliasResolver resolver) {
            return new AliasedQueryVisitor(resolver).Accept(node, null);
        }

        public static IQueryNode Run(GroupNode node, AliasMap aliasMap) {
            return new AliasedQueryVisitor(aliasMap.Resolve).Accept(node, null);
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

            // field could be multi-part "level1.level2.level3:value"
            // break it into parts and try to resolve each level "level1", "level1.level2"
            // if we get a result from breaking out the parts, then the returned resolver needs to be wrapped with a prefix
            // "level1" is mapped to "blah", the returned resolver would be for level1, but any child aliases should be for "level1.level2.level3".
            // so we would need to use the resolver for "level1" since that is all we have and any requested resolvers would get prefixed with "level2.level3."
            // if we get a result from breaking out the parts, then we need to try and resolve the rest of the parts so that we can get the deepest match and resolver

            //var fieldParts = field.Split('.');
            //if (fieldParts.Length == 1)
            //    return null;

            //call current resolver with 1st part


            //var partsToCheck = new List<Tuple<string, int>>();
            //for (int i = 0; i < fieldParts.Length - 1; i++)
            //    partsToCheck.Add(Tuple.Create(String.Join(".", fieldParts.Take(i + 1)), i));

            //Func<string, GetAliasResult> currentResolver = Resolve;
            //GetAliasResult currentResult = null;
            //for (int i = 0; i < partsToCheck.Count; i++) {
            //    var part = partsToCheck[i];
            //    currentResult = currentResolver(part.Item1);
            //    if (currentResult == null)
            //        break;

            //    string suffix = String.Join(".", fieldParts.Skip(i + 1));
            //    if (suffix.Length > 0)
            //        currentResult.Name = currentResult.Name + "." + suffix;

            //    var resolver = currentResult.Resolver;
            //    var result = resolver(String.Join(".", fieldParts.Skip(i + 1)));
            //    if (result == null)
            //        string prefix = String.Join(".", fieldParts.Take(i));
            //    if (prefix.Length > 0)
            //        currentResult.Resolver = AliasedQueryVisitor.GetScopedResolver(resolver, prefix);
            //}

            //return currentResult;
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

        private static readonly AliasResolver EmptyResolve = f => (GetAliasResult)null;
    }

    public class AliasMapValue {
        public string Name { get; set; }

        public bool HasChildMappings => _aliasMap != null && _aliasMap.Count > 0;

        private AliasMap _aliasMap;
        public AliasMap ChildMap => _aliasMap ?? (_aliasMap = new AliasMap());
    }
}

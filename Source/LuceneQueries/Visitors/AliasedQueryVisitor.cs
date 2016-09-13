using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class AliasedQueryVisitor : ChainableQueryVisitor {
        private readonly Stack<AliasMap> _aliasMapStack = new Stack<AliasMap>();

        public AliasedQueryVisitor(AliasMap aliasMap) {
            _aliasMapStack.Push(aliasMap);
        }

        public override void Visit(GroupNode node) {
            var alias = GetFieldAlias(node.Field);
            node.Field = alias.Item1;

            _aliasMapStack.Push(alias.Item2 ?? _aliasMapStack.Peek());

            foreach (var child in node.Children)
                child.Accept(this);

            _aliasMapStack.Pop();
        }

        public override void Visit(TermNode node) {
            node.Field = GetFieldAlias(node.Field).Item1;
        }

        public override void Visit(TermRangeNode node) {
            node.Field = GetFieldAlias(node.Field).Item1;
        }

        public override void Visit(ExistsNode node) {
            node.Field = GetFieldAlias(node.Field).Item1;
        }

        public override void Visit(MissingNode node) {
            node.Field = GetFieldAlias(node.Field).Item1;
        }

        private Tuple<string, AliasMap> GetFieldAlias(string name) {
            if (String.IsNullOrEmpty(name))
                return Tuple.Create(name, (AliasMap)null);

            string[] fieldParts = name.Split('.');
            string fieldName = String.Empty;
            AliasMap currentMap = _aliasMapStack.Peek();
            for (int i = 0; i < fieldParts.Length; i++) {
                AliasMapValue aliasMapValue;
                if (i > 0)
                    fieldName += ".";

                if (currentMap != null && currentMap.TryGetValue(fieldParts[i], out aliasMapValue)) {
                    fieldName += aliasMapValue.Name;
                    currentMap = aliasMapValue.HasChildMappings ? aliasMapValue.ChildMap : null;
                } else {
                    fieldName += fieldParts[i];
                    currentMap = null;
                }
            }

            return Tuple.Create(fieldName.Length > 0 ? fieldName : name, currentMap);
        }

        public override IQueryNode Accept(IQueryNode node) {
            node.Accept(this);
            return node;
        }

        public static IQueryNode Run(IQueryNode node, AliasMap aliasMap) {
            return new AliasedQueryVisitor(aliasMap).Accept(node);
        }
    }

    public class AliasMap : Dictionary<string, AliasMapValue> {
        public void Add(string key, string value) {
            Add(key, new AliasMapValue { Name = value });
        }
    }

    public class AliasMapValue {
        public string Name { get; set; }

        public bool HasChildMappings => _aliasMap != null && _aliasMap.Count > 0;

        private AliasMap _aliasMap;
        public AliasMap ChildMap => _aliasMap ?? (_aliasMap = new AliasMap());
    }
}

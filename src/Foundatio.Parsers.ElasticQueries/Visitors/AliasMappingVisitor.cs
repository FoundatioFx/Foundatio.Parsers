using System;
using System.Collections.Generic;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class AliasMappingVisitor : NoopMappingVisitor {
        private readonly Inferrer _inferrer;
        private readonly Stack<AliasMapValue> _stack = new Stack<AliasMapValue>();

        public AliasMappingVisitor(Inferrer inferrer) {
            _inferrer = inferrer;
        }

        public AliasMap RootAliasMap { get; } = new AliasMap();

        public override void Visit(ITextProperty property) => AddAlias(property);

        public override void Visit(IKeywordProperty property) => AddAlias(property);

        public override void Visit(IDateProperty property) => AddAlias(property);

        public override void Visit(IBooleanProperty property) => AddAlias(property);

        public override void Visit(IBinaryProperty property) => AddAlias(property);

        public override void Visit(IObjectProperty property) => AddAlias(property);

        public override void Visit(INestedProperty property) => AddAlias(property);

        public override void Visit(IIpProperty property) => AddAlias(property);

        public override void Visit(IGeoPointProperty property) => AddAlias(property);

        public override void Visit(IGeoShapeProperty property) => AddAlias(property);

        public override void Visit(IAttachmentProperty property) => AddAlias(property);

        public override void Visit(INumberProperty property) => AddAlias(property);

        public override void Visit(ICompletionProperty property) => AddAlias(property);

        public override void Visit(IMurmur3HashProperty property) => AddAlias(property);

        public override void Visit(ITokenCountProperty property) => AddAlias(property);

        private void AddAlias(IProperty property) {
            while (Depth < _stack.Count)
                _stack.Pop();

            var map = Depth == 0 ? RootAliasMap : _stack.Peek().ChildMap;
            string name = _inferrer.PropertyName(property.Name);
            var amv = new AliasMapValue { Name = name };

            var alias = property.GetAlias();
            if (alias.HasValue) {
                if (alias.Value.Value)
                    RootAliasMap.Add(alias.Value.Key, new AliasMapValue { Name = GetFullPath(amv.Name, _stack) });
                else
                    map.Add(alias.Value.Key, amv);
            }

            var objectProperty = property as IObjectProperty;
            if (objectProperty != null) {
                if (!alias.HasValue || !String.Equals(name, alias.Value.Key, StringComparison.OrdinalIgnoreCase))
                    map.Add(name, amv);

                if (objectProperty.Properties != null)
                    _stack.Push(amv);
            }
        }

        private string GetFullPath(string name, Stack<AliasMapValue> stack) {
            if (stack.Count == 0)
                return name;

            string path = name;
            var items = stack.ToArray();
            for (int index = items.Length - 1; index >= 0; index--)
                path = $"{items[index].Name}.{path}";

            return path;
        }
    }
}
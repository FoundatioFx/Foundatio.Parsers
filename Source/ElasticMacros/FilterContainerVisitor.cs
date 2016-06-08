using System;
using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Nest;

namespace ElasticMacros.Visitor {
    public class FilterContainerVisitor : QueryNodeVisitorWithResultBase<FilterContainer> {
        private readonly Stack<Operator> _defaultOperatorStack = new Stack<Operator>();
        private readonly Stack<string> _defaultFieldStack = new Stack<string>();
        private readonly Operator _defaultOperator;
        private FilterContainer _filter;
        private readonly List<IElasticMacro> _macros = new List<IElasticMacro>();

        public FilterContainerVisitor(ElasticMacrosConfiguration config) {
            _defaultOperator = config.DefaultOperator;
            _defaultFieldStack.Push(config.DefaultField);
            _macros.AddRange(config.Macros);
        }

        public override void Visit(GroupNode node) {
            FilterContainer parent = null;
            if (node.HasParens) {
                parent = _filter;
                _filter = null;
            }

            _defaultOperatorStack.Push(GetOperator(node));
            if (!String.IsNullOrEmpty(node.Field))
                _defaultFieldStack.Push(node.Field);

            foreach (var child in node.Children)
                child.Accept(this, false);

            _defaultOperatorStack.Pop();
            if (!String.IsNullOrEmpty(node.Field))
                _defaultFieldStack.Pop();

            if (parent == null)
                return;

            var op = _defaultOperatorStack.Peek();
            if (op == Operator.And)
                parent &= _filter;
            else
                parent |= _filter;

            _filter = parent;
        }

        public override void Visit(TermNode node) {
            PlainFilter filter = new TermFilter { Field = node.Field ?? _defaultFieldStack.Peek(), Value = node.Term };
            foreach (var macro in _macros)
                filter = macro.Expand(node, filter, new ElasticMacroContext { DefaultField = _defaultFieldStack.Peek() });

            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                _filter &= !filter.ToContainer();
            else if (_defaultOperatorStack.Peek() == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                _filter &= filter.ToContainer();
            else
                _filter |= filter.ToContainer();
        }

        public override void Visit(TermRangeNode node) {
            var range = new RangeFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
            if (!String.IsNullOrWhiteSpace(node.Min)) {
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.GreaterThan = node.Min;
                else
                    range.GreaterThanOrEqualTo = node.Min;
            }

            if (!String.IsNullOrWhiteSpace(node.Max)) {
                if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                    range.LowerThan = node.Max;
                else
                    range.LowerThanOrEqualTo = node.Max;
            }

            PlainFilter filter = range;
            foreach (var macro in _macros)
                filter = macro.Expand(node, filter, new ElasticMacroContext { DefaultField = _defaultFieldStack.Peek() });

            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                _filter &= !filter.ToContainer();
            if (_defaultOperatorStack.Peek() == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                _filter &= filter.ToContainer();
            else
                _filter |= filter.ToContainer();
        }

        public override void Visit(ExistsNode node) {
            PlainFilter filter = new ExistsFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
            foreach (var macro in _macros)
                filter = macro.Expand(node, filter, new ElasticMacroContext { DefaultField = _defaultFieldStack.Peek() });

            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                _filter &= !filter.ToContainer();
            else if (_defaultOperatorStack.Peek() == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                _filter &= filter.ToContainer();
            else
                _filter |= filter.ToContainer();
        }

        public override void Visit(MissingNode node) {
            PlainFilter filter = new MissingFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
            foreach (var macro in _macros)
                filter = macro.Expand(node, filter, new ElasticMacroContext { DefaultField = _defaultFieldStack.Peek() });

            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                _filter &= !filter.ToContainer();
            else if (_defaultOperatorStack.Peek() == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                _filter &= filter.ToContainer();
            else
                _filter |= filter.ToContainer();
        }

        public override FilterContainer Accept(IQueryNode node) {
            node.Accept(this, false);
            if (_filter != null)
                return _filter;

            return new MatchAllFilter().ToContainer();
        }

        private Operator GetOperator(GroupNode node) {
            switch (node.Operator) {
                case GroupOperator.And:
                case GroupOperator.AndNot:
                    return Operator.And;
                case GroupOperator.Or:
                case GroupOperator.OrNot:
                    return Operator.Or;
            }

            return _defaultOperator;
        }
    }
}

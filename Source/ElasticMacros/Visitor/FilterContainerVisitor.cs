using System;
using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Nest;

namespace ElasticMacros.Visitor {
    public class FilterContainerVisitor : QueryNodeVisitorBase<FilterContainer> {
        private readonly Stack<Operator> _defaultOperatorStack = new Stack<Operator>();
        private readonly Stack<string> _defaultFieldStack = new Stack<string>();
        private readonly Operator _defaultOperator;
        private FilterContainer _filter;

        public FilterContainerVisitor(Operator defaultOperator, string defaultField) {
            _defaultOperator = defaultOperator;
            _defaultFieldStack.Push(defaultField);
        }

        public override void Visit(GroupNode node) {
            FilterContainer parent = null;
            if (_filter == null || node.HasParens) {
                parent = _filter;
                _filter = new FilterContainer();
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
            var op = _defaultOperatorStack.Peek();
            
            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                _filter &= !FilterContainer.From(new TermFilter { Field = node.Field ?? _defaultFieldStack.Peek(), Value = node.Term });
            else if (op == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                _filter &= new TermFilter { Field = node.Field ?? _defaultFieldStack.Peek(), Value = node.Term };
            else
                _filter |= new TermFilter { Field = node.Field ?? _defaultFieldStack.Peek(), Value = node.Term };
        }

        public override void Visit(TermRangeNode node) {
            var op = _defaultOperatorStack.Peek();

            if (op == Operator.And) {
                var range = new RangeFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.GreaterThan = node.Min;
                else
                    range.GreaterThanOrEqualTo = node.Min;

                _filter &= range;
            } else {
                var range = new RangeFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
                _filter |= range;
            }
        }

        public override void Visit(ExistsNode node) {
            var op = _defaultOperatorStack.Peek();

            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                _filter &= !FilterContainer.From(new ExistsFilter { Field = node.Field ?? _defaultFieldStack.Peek() });
            else if (op == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                _filter &= new ExistsFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
            else
                _filter |= new ExistsFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
        }

        public override void Visit(MissingNode node) {
            var op = _defaultOperatorStack.Peek();

            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                _filter &= !FilterContainer.From(new MissingFilter { Field = node.Field ?? _defaultFieldStack.Peek() });
            else if (op == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                _filter &= new MissingFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
            else
                _filter |= new MissingFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
        }

        public override FilterContainer Accept(IQueryNode node) {
            node.Accept(this, false);
            return _filter;
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

        public static FilterContainer Run(IQueryNode node, Operator defaultOperator = Operator.And, string defaultField = null) {
            return new FilterContainerVisitor(defaultOperator, defaultField).Accept(node);
        }
    }
}

using System;
using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Nest;

namespace ElasticMacros.Visitor {
    public class FilterContainerVisitor : QueryNodeVisitorBase {
        private readonly Stack<Operator> _defaultOperatorStack = new Stack<Operator>();
        private readonly Stack<string> _defaultFieldStack = new Stack<string>();

        public FilterContainerVisitor(Operator defaultOperator, string defaultField) {
            DefaultOperator = defaultOperator;
            DefaultField = defaultField;
        }

        public FilterContainer Filter { get; private set; }
        public Operator DefaultOperator { get; private set; }
        public string DefaultField { get; private set; }

        public override void Visit(GroupNode node) {
            FilterContainer parent = null;
            if (Filter == null || node.HasParens) {
                parent = Filter;
                Filter = new FilterContainer();
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
                parent &= Filter;
            else
                parent |= Filter;

            Filter = parent;
        }

        public override void Visit(TermNode node) {
            var op = _defaultOperatorStack.Peek();
            
            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                Filter &= !FilterContainer.From(new TermFilter { Field = node.Field ?? _defaultFieldStack.Peek(), Value = node.Term });
            else if (op == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                Filter &= new TermFilter { Field = node.Field ?? _defaultFieldStack.Peek(), Value = node.Term };
            else
                Filter |= new TermFilter { Field = node.Field ?? _defaultFieldStack.Peek(), Value = node.Term };
        }

        public override void Visit(TermRangeNode node) {
            var op = _defaultOperatorStack.Peek();

            if (op == Operator.And) {
                var range = new RangeFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.GreaterThan = node.Min;
                else
                    range.GreaterThanOrEqualTo = node.Min;

                Filter &= range;
            } else {
                var range = new RangeFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
                Filter |= range;
            }
        }

        public override void Visit(ExistsNode node) {
            var op = _defaultOperatorStack.Peek();

            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                Filter &= !FilterContainer.From(new ExistsFilter { Field = node.Field ?? _defaultFieldStack.Peek() });
            else if (op == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                Filter &= new ExistsFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
            else
                Filter |= new ExistsFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
        }

        public override void Visit(MissingNode node) {
            var op = _defaultOperatorStack.Peek();

            if (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-")
                Filter &= !FilterContainer.From(new MissingFilter { Field = node.Field ?? _defaultFieldStack.Peek() });
            else if (op == Operator.And || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+"))
                Filter &= new MissingFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
            else
                Filter |= new MissingFilter { Field = node.Field ?? _defaultFieldStack.Peek() };
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

            return DefaultOperator;
        }

        public static FilterContainer Run(IQueryNode node, Operator defaultOperator = Operator.And, string defaultField = null) {
            var visitor = new FilterContainerVisitor(defaultOperator, defaultField);
            node.Accept(visitor, false);
            return visitor.Filter;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Nest;

namespace ElasticMacros.QueryMacros {
    public class QueryContainerVisitor : QueryNodeVisitorWithResultBase<QueryContainer> {
        private readonly Stack<Operator> _operatorStack = new Stack<Operator>();
        private readonly Stack<string> _defaultFieldStack = new Stack<string>();
        private QueryContainer _query;
        private readonly ElasticMacrosConfiguration _config;

        public QueryContainerVisitor(ElasticMacrosConfiguration config) {
            _config = config;
            _defaultFieldStack.Push(config.DefaultField);
        }

        public override void Visit(GroupNode node) {
            QueryContainer parent = null;
            if (node.HasParens) {
                parent = _query;
                _query = null;
            }

            _operatorStack.Push(GetOperator(node));
            if (!String.IsNullOrEmpty(node.Field))
                _defaultFieldStack.Push(node.Field);

            foreach (var child in node.Children)
                child.Accept(this, false);

            _operatorStack.Pop();
            if (!String.IsNullOrEmpty(node.Field))
                _defaultFieldStack.Pop();

            if (parent == null)
                return;

            AddQuery(ref parent, _query, node.IsNegated, node.Prefix);
            _query = parent;
        }

        public override void Visit(TermNode node) {
            PlainQuery query = null;
            if (_config.IsFieldAnalyzed(GetFullFieldName(node.Field))) {
                query = new QueryStringQuery {
                    Query = node.IsQuotedTerm ? "\"" + node.UnescapedTerm + "\"" : node.UnescapedTerm,
                    DefaultField = node.Field,
                    DefaultOperator = _operatorStack.Peek()
                };
            } else {
                query = new TermQuery {
                    Field = node.Field ?? _defaultFieldStack.Peek(),
                    Value = _config.TransformTerm(node.Field, node.UnescapedTerm)
                };
            }

            var ctx = new ElasticQueryMacroContext {
                DefaultField = _defaultFieldStack.Peek(),
                Query = query
            };

            foreach (var macro in _config.QueryMacros)
                macro.Expand(node, ctx);

            AddQuery(ctx.Query, node.IsNegated, node.Prefix);
        }

        public override void Visit(TermRangeNode node) {
            if (_config.IsFieldAnalyzed(GetFullFieldName(node.Field)))
                return;

            var range = new RangeQuery { Field = node.Field ?? _defaultFieldStack.Peek() };
            if (!String.IsNullOrWhiteSpace(node.UnescapedMin)) {
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.GreaterThan = node.UnescapedMin;
                else
                    range.GreaterThanOrEqualTo = node.UnescapedMin;
            }

            if (!String.IsNullOrWhiteSpace(node.UnescapedMax)) {
                if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                    range.LowerThan = node.UnescapedMax;
                else
                    range.LowerThanOrEqualTo = node.UnescapedMax;
            }

            PlainQuery query = range;
            var ctx = new ElasticQueryMacroContext {
                DefaultField = _defaultFieldStack.Peek(),
                Query = query
            };

            foreach (var macro in _config.QueryMacros)
                macro.Expand(node, ctx);

            AddQuery(ctx.Query, node.IsNegated, node.Prefix);
        }

        public override void Visit(ExistsNode node) {
            if (_config.IsFieldAnalyzed(GetFullFieldName(node.Field)))
                return;

            PlainQuery query = new FilteredQuery {
                Filter = new ExistsFilter { Field = node.Field ?? _defaultFieldStack.Peek() }.ToContainer()
            };
            var ctx = new ElasticQueryMacroContext {
                DefaultField = _defaultFieldStack.Peek(),
                Query = query
            };

            foreach (var macro in _config.QueryMacros)
                macro.Expand(node, ctx);
            
            AddQuery(ctx.Query, node.IsNegated, node.Prefix);
        }

        public override void Visit(MissingNode node) {
            if (_config.IsFieldAnalyzed(GetFullFieldName(node.Field)))
                return;

            PlainQuery filter = new FilteredQuery {
                Filter = new MissingFilter { Field = node.Field ?? _defaultFieldStack.Peek() }.ToContainer()
            };
            var ctx = new ElasticQueryMacroContext {
                DefaultField = _defaultFieldStack.Peek(),
                Query = filter
            };

            foreach (var macro in _config.QueryMacros)
                macro.Expand(node, ctx);

            AddQuery(ctx.Query, node.IsNegated, node.Prefix);
        }

        private string GetFullFieldName(string field) {
            if (_defaultFieldStack.Count == 1)
                return field;

            var sb = new StringBuilder();
            // skip 1st field in stack because that is the default field and not a group field
            foreach (var f in _defaultFieldStack.Skip(1)) {
                sb.Append(f);
                sb.Append('.');
            }

            sb.Append(field);

            return sb.ToString();
        }

        private void AddQuery(PlainQuery query, bool? isNegated, string prefix) {
            AddQuery(ref _query, query.ToContainer(), isNegated, prefix);
        }

        private void AddQuery(ref QueryContainer target, QueryContainer container, bool? isNegated, string prefix) {
            var op = _operatorStack.Peek();
            if ((isNegated.HasValue && isNegated.Value)
                || (!String.IsNullOrEmpty(prefix) && prefix == "-"))
                container = !container;

            if (op == Operator.Or && !String.IsNullOrEmpty(prefix) && prefix == "+")
                op = Operator.And;

            if (op == Operator.And) {
                target &= container;
            } else if (op == Operator.Or) {
                target |= container;
            }
        }

        public override QueryContainer Accept(IQueryNode node) {
            node.Accept(this, false);
            if (_query != null)
                return _query;

            return new MatchAllQuery().ToContainer();
        }

        private Operator GetOperator(GroupNode node) {
            switch (node.Operator) {
                case GroupOperator.And:
                    return Operator.And;
                case GroupOperator.Or:
                    return Operator.Or;
                default:
                    return _config.DefaultQueryOperator;
            }
        }
    }
}

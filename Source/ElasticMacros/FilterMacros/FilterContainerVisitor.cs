using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Nest;

namespace ElasticMacros.FilterMacros {
    public class FilterContainerVisitor : QueryNodeVisitorWithResultBase<FilterContainer> {
        private readonly ElasticMacrosConfiguration _config;
        private ElasticFilterMacroContext _context;

        public FilterContainerVisitor(ElasticMacrosConfiguration config) {
            _config = config;
        }

        public override void Visit(GroupNode node) {
            ElasticFilterMacroContext parent = null;
            if (node.HasParens || _context == null) {
                if (_context != null)
                    parent = _context;

                _context = new ElasticFilterMacroContext {
                    Config = _config,
                    DefaultField = !String.IsNullOrEmpty(node.Field) ? node.Field : parent?.DefaultField ?? _config.DefaultField,
                    Group = node,
                    Operator = GetOperator(node),
                    FieldPrefixParts = parent?.FieldPrefixParts ?? new string[] {}
                };

                if (!String.IsNullOrEmpty(node.Field)) {
                    var prefixParts = parent?.FieldPrefixParts?.ToList() ?? new List<string>();
                    prefixParts.Add(node.Field);
                    _context.FieldPrefixParts = prefixParts.ToArray();
                }
            }

            foreach (var child in node.Children)
                child.Accept(this, false);

            if (parent == null)
                return;

            var current = _context;
            _context = parent;
            AddFilter(current.Filter, node.IsNegated, node.Prefix);
        }

        public override void Visit(TermNode node) {
            FilterContainer filter = null;
            if (_config.IsFieldAnalyzed(_context.GetFullFieldName(node.Field))) {
                filter = new QueryFilter { Query = new QueryStringQuery {
                    Query = node.IsQuotedTerm ? "\"" + node.UnescapedTerm + "\"" : node.UnescapedTerm,
                    DefaultField = node.Field,
                    AllowLeadingWildcard = false,
                    AnalyzeWildcard = true,
                    DefaultOperator = _context.Operator
                }.ToContainer() };
            } else {
                filter = new TermFilter {
                    Field = node.Field ?? _context.DefaultField,
                    Value = node.UnescapedTerm
                };
            }

            var ctx = new ElasticFilterMacroContext {
                DefaultField = _context.DefaultField,
                Filter = filter,
                FieldPrefixParts = _context.FieldPrefixParts,
                Operator = _context.Operator,
                Config = _context.Config,
                Group = _context.Group
            };

            foreach (var macro in _config.FilterMacros)
                macro.Expand(node, ctx);

            AddFilter(ctx.Filter, node.IsNegated, node.Prefix);
        }

        public override void Visit(TermRangeNode node) {
            var range = new RangeFilter { Field = node.Field ?? _context.DefaultField };
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

            FilterContainer filter = range;
            var ctx = new ElasticFilterMacroContext {
                DefaultField = _context.DefaultField,
                Filter = filter,
                FieldPrefixParts = _context.FieldPrefixParts,
                Operator = _context.Operator,
                Config = _context.Config,
                Group = _context.Group
            };

            foreach (var macro in _config.FilterMacros)
                macro.Expand(node, ctx);

            AddFilter(ctx.Filter, node.IsNegated, node.Prefix);
        }

        public override void Visit(ExistsNode node) {
            FilterContainer filter = new ExistsFilter { Field = node.Field ?? _context.DefaultField };
            var ctx = new ElasticFilterMacroContext {
                DefaultField = _context.DefaultField,
                Filter = filter,
                FieldPrefixParts = _context.FieldPrefixParts,
                Operator = _context.Operator,
                Config = _context.Config,
                Group = _context.Group
            };

            foreach (var macro in _config.FilterMacros)
                macro.Expand(node, ctx);

            AddFilter(ctx.Filter, node.IsNegated, node.Prefix);
        }

        public override void Visit(MissingNode node) {
            FilterContainer filter = new MissingFilter { Field = node.Field ?? _context.DefaultField };
            var ctx = new ElasticFilterMacroContext {
                DefaultField = _context.DefaultField,
                Filter = filter,
                FieldPrefixParts = _context.FieldPrefixParts,
                Operator = _context.Operator,
                Config = _context.Config,
                Group = _context.Group
            };

            foreach (var macro in _config.FilterMacros)
                macro.Expand(node, ctx);

            AddFilter(ctx.Filter, node.IsNegated, node.Prefix);
        }

        private void AddFilter(FilterContainer container, bool? isNegated, string prefix) {
            var op = _context.Operator;
            if ((isNegated.HasValue && isNegated.Value)
                || (!String.IsNullOrEmpty(prefix) && prefix == "-"))
                container = !container;

            if (op == Operator.Or && !String.IsNullOrEmpty(prefix) && prefix == "+")
                op = Operator.And;

            if (op == Operator.And) {
                _context.Filter &= container;
            } else if (op == Operator.Or) {
                _context.Filter |= container;
            }
        }

        public override FilterContainer Accept(IQueryNode node) {
            node.Accept(this, false);
            if (_context.Filter != null)
                return _context.Filter;

            return new MatchAllFilter().ToContainer();
        }

        private Operator GetOperator(GroupNode node) {
            switch (node.Operator) {
                case GroupOperator.And:
                    return Operator.And;
                case GroupOperator.Or:
                    return Operator.Or;
                default:
                    return _config.DefaultFilterOperator;
            }
        }
    }
}

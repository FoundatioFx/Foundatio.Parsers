using System;
using Foundatio.Parsers.ElasticQueries.Filter.Nodes;
using Foundatio.Parsers.ElasticQueries.Query.Nodes;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class QueryNodeExtensions {
        public static void InvalidateFilter(this IQueryNode node) {
            IQueryNode current = node;
            while (current != null) {
                var filterNode = current as FilterGroupNode;
                if (filterNode != null)
                    filterNode.Filter = null;

                current = current.Parent;
            }
        }

        public static void InvalidateQuery(this IQueryNode node) {
            IQueryNode current = node;
            while (current != null) {
                var filterNode = current as QueryGroupNode;
                if (filterNode != null)
                    filterNode.Query = null;

                current = current.Parent;
            }
        }

        public static Operator GetOperator(this IQueryNode node, Operator defaultOperator) {
            var groupNode = node as GroupNode;
            if (groupNode == null)
                return defaultOperator;

            switch (groupNode.Operator) {
                case GroupOperator.And:
                    return Operator.And;
                case GroupOperator.Or:
                    return Operator.Or;
                default:
                    return defaultOperator;
            }
        }

        public static IQueryNode ToFilter(this IQueryNode node) {
            var groupNode = node as GroupNode;
            if (groupNode != null) {
                var filterNode = new FilterGroupNode();
                groupNode.CopyTo(filterNode);

                if (filterNode.Left != null) {
                    filterNode.Left = filterNode.Left.ToFilter();
                    filterNode.Left.Parent = filterNode;
                }

                if (filterNode.Right != null) {
                    filterNode.Right = filterNode.Right.ToFilter();
                    filterNode.Right.Parent = filterNode;
                }

                return filterNode;
            }

            var termNode = node as TermNode;
            if (termNode != null) {
                var filterNode = new FilterTermNode();
                termNode.CopyTo(filterNode);
                return filterNode;
            }

            var termRangeNode = node as TermRangeNode;
            if (termRangeNode != null) {
                var filterNode = new FilterTermRangeNode();
                termRangeNode.CopyTo(filterNode);
                return filterNode;
            }

            var missingNode = node as MissingNode;
            if (missingNode != null) {
                var filterNode = new FilterMissingNode();
                missingNode.CopyTo(filterNode);
                return filterNode;
            }

            var existsNode = node as ExistsNode;
            if (existsNode != null) {
                var filterNode = new FilterExistsNode();
                existsNode.CopyTo(filterNode);
                return filterNode;
            }

            return null;
        }

        public static IQueryNode ToQuery(this IQueryNode node) {
            var groupNode = node as GroupNode;
            if (groupNode != null) {
                var queryNode = new QueryGroupNode();
                groupNode.CopyTo(queryNode);

                if (queryNode.Left != null) {
                    queryNode.Left = queryNode.Left.ToQuery();
                    queryNode.Left.Parent = queryNode;
                }

                if (queryNode.Right != null) {
                    queryNode.Right = queryNode.Right.ToQuery();
                    queryNode.Right.Parent = queryNode;
                }

                return queryNode;
            }

            var termNode = node as TermNode;
            if (termNode != null) {
                var queryNode = new QueryTermNode();
                termNode.CopyTo(queryNode);
                return queryNode;
            }

            var termRangeNode = node as TermRangeNode;
            if (termRangeNode != null) {
                var queryNode = new QueryTermRangeNode();
                termRangeNode.CopyTo(queryNode);
                return queryNode;
            }

            var missingNode = node as MissingNode;
            if (missingNode != null) {
                var queryNode = new QueryMissingNode();
                missingNode.CopyTo(queryNode);
                return queryNode;
            }

            var existsNode = node as ExistsNode;
            if (existsNode != null) {
                var queryNode = new QueryExistsNode();
                existsNode.CopyTo(queryNode);
                return queryNode;
            }

            return null;
        }
    }
}

using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Filter;
using Foundatio.Parsers.ElasticQueries.Query;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParser {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();
        private readonly ChainedQueryVisitor _filterVisitor = new ChainedQueryVisitor();
        private readonly ChainedQueryVisitor _queryVisitor = new ChainedQueryVisitor();
        private readonly DefaultFilterVisitor _defaultFilterVisitor;
        private readonly DefaultQueryVisitor _defaultQueryVisitor;
        private readonly CombineFiltersVisitor _combineFiltersVisitor;
        private readonly CombineQueriesVisitor _combineQueriesVisitor;

        public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);

            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _filterVisitor.AddVisitor(visitor);

            _defaultFilterVisitor = new DefaultFilterVisitor(config);
            _combineFiltersVisitor = new CombineFiltersVisitor(config);

            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _queryVisitor.AddVisitor(visitor);

            _defaultQueryVisitor = new DefaultQueryVisitor(config);
            _combineQueriesVisitor = new CombineQueriesVisitor(config);
        }

        public FilterContainer BuildFilter(string query) {
            var result = _parser.Parse(query);

            var context = new ElasticQueryVisitorContext();
            var filterNode = _defaultFilterVisitor.Accept(result, context);
            filterNode = _filterVisitor.Accept(filterNode, context);
            filterNode = _combineFiltersVisitor.Accept(filterNode, context);

            return filterNode?.GetFilterContainer() ?? new MatchAllFilter();
        }

        public QueryContainer BuildQuery(string query) {
            var result = _parser.Parse(query);

            var context = new ElasticQueryVisitorContext();
            var queryNode = _defaultQueryVisitor.Accept(result, context);
            queryNode = _queryVisitor.Accept(queryNode, context);
            queryNode = _combineQueriesVisitor.Accept(queryNode, context);

            return queryNode?.GetQueryContainer() ?? new MatchAllQuery();
        }

        // parser query, generate filter, generate aggregations
        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
        // _exists_:field1
        // automatic field alias management
    }
}

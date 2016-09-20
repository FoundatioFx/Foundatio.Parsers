using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Filter;
using Foundatio.Parsers.ElasticQueries.Query;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParser {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();
        private readonly ChainedQueryVisitor _filterVisitor = new ChainedQueryVisitor();
        private readonly ChainedQueryVisitor _queryVisitor = new ChainedQueryVisitor();

        public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);

            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _filterVisitor.AddVisitor(visitor);
            _filterVisitor.AddVisitor(new FilterContainerVisitor(config), 100);
            _filterVisitor.AddVisitor(new CombineFiltersVisitor(config), 100000);

            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _queryVisitor.AddVisitor(visitor);
            _queryVisitor.AddVisitor(new QueryContainerVisitor(config), 100);
            _queryVisitor.AddVisitor(new CombineQueriesVisitor(config), 100000);
        }

        public FilterContainer BuildFilter(string query) {
            var result = _parser.Parse(query);

            var filterNode = _filterVisitor.Accept(result);

            return filterNode?.GetFilter() ?? new MatchAllFilter();
        }

        public QueryContainer BuildQuery(string query) {
            var result = _parser.Parse(query);

            var queryNode = _queryVisitor.Accept(result);

            return queryNode?.GetQuery() ?? new MatchAllQuery();
        }

        // parser query, generate filter, generate aggregations
        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
        // _exists_:field1
        // automatic field alias management
    }
}

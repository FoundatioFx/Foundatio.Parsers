using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Query;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParser {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();
        private readonly ChainedQueryVisitor _queryVisitor = new ChainedQueryVisitor();

        public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);

            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _queryVisitor.AddVisitor(visitor);
            _queryVisitor.AddVisitor(new QueryContainerVisitor(config), 100);
            _queryVisitor.AddVisitor(new CombineQueriesVisitor(config), 100000);
        }

        public QueryContainer BuildFilter(string filter) {
            var result = _parser.Parse(filter);

            var queryNode = _queryVisitor.Accept(result);
            var q = queryNode?.GetQuery() ?? new MatchAllQuery();
            q = new BoolQuery {
                Filter = new QueryContainer[] { q }
            };

            return q;
        }

        public QueryContainer BuildQuery(string query) {
            var result = _parser.Parse(query);

            var queryNode = _queryVisitor.Accept(result);
            var q = queryNode?.GetQuery() ?? new MatchAllQuery();

            return q;
        }

        // parser query, generate filter, generate aggregations
        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
        // _exists_:field1
        // automatic field alias management
    }
}

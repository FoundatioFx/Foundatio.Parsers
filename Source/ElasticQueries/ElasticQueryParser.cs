using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Query;
using Foundatio.Parsers.ElasticQueries.Visitors;
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

        public QueryContainer BuildQuery(string query, Operator defaultOperator = Operator.And, bool scoreResults = false) {
            var result = _parser.Parse(query);

            var context = new ElasticQueryVisitorContext();
            context.SetDefaultOperator(defaultOperator);
            var queryNode = _queryVisitor.Accept(result, context);
            var q = queryNode?.GetQuery() ?? new MatchAllQuery();
            if (!scoreResults) {
                q = new BoolQuery {
                    Filter = new QueryContainer[] { q }
                };
            }

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

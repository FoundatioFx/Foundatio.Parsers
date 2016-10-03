using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParser {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();
        private readonly ChainedQueryVisitor _queryVisitor = new ChainedQueryVisitor();
        private readonly DefaultQueryVisitor _defaultQueryVisitor;
        private readonly CombineQueriesVisitor _combineQueriesVisitor;

        public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);

            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _queryVisitor.AddVisitor(visitor);

            _defaultQueryVisitor = new DefaultQueryVisitor(config);
            _combineQueriesVisitor = new CombineQueriesVisitor(config);
        }

        public QueryContainer BuildQuery(string query, Operator defaultOperator = Operator.And, bool scoreResults = false) {
            var result = _parser.Parse(query);

            var context = new ElasticQueryVisitorContext();
            context.SetDefaultOperator(defaultOperator);

            var queryNode = _defaultQueryVisitor.Accept(result, context);
            queryNode = _queryVisitor.Accept(queryNode, context);
            queryNode = _combineQueriesVisitor.Accept(queryNode, context);

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

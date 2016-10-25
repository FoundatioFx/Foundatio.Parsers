using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParser {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();
        private readonly ElasticQueryParserConfiguration _config;

        public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);
            _config = config;
        }

        public QueryContainer BuildQuery(string query, IElasticQueryVisitorContext context = null) {
            var result = _parser.Parse(query);

            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.SetGetPropertyMappingFunc(_config.GetMappingProperty)
                .SetDefaultOperator(Operator.Or)
                .SetDefaultField(_config.DefaultField);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            var queryNode = _config.QueryVisitor.Accept(result, context);

            var q = queryNode?.GetQuery() ?? new MatchAllQuery();
            if (!context.UseScoring) {
                q = new BoolQuery {
                    Filter = new QueryContainer[] { q }
                };
            }
			
			return q;
		}	

        public AggregationContainer BuildAggregations(string aggregations, IElasticQueryVisitorContext context = null) {
            var result = _parser.Parse(aggregations);

            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.SetGetPropertyMappingFunc(_config.GetMappingProperty);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            var queryNode = _config.AggregationVisitor.Accept(result, context);

            return queryNode?.GetAggregation();
        }

        // parser query, generate filter, generate aggregations
        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
        // _exists_:field1
        // automatic field alias management
    }
}

using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Nest;
using Foundatio.Parsers.LuceneQueries.Nodes;

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
            return BuildQuery(result, context);
        }


        public QueryContainer BuildQuery(GroupNode query, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.SetGetFieldMappingFunc(_config.GetFieldMapping)
                .SetDefaultOperator(Operator.Or)
                .SetDefaultField(_config.DefaultField);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            var queryNode = _config.QueryVisitor.Accept(query, context);

            return queryNode?.GetQueryContainer() ?? new MatchAllQuery();
        }

        public AggregationContainer BuildAggregations(string aggregations, IElasticQueryVisitorContext context = null) {
            var result = _parser.Parse(aggregations);
            return BuildAggregations(result, context);
        }

        public AggregationContainer BuildAggregations(GroupNode aggregations, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.SetGetFieldMappingFunc(_config.GetFieldMapping);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            var queryNode = _config.AggregationVisitor.Accept(aggregations, context);

            var namedAggregation = queryNode?.GetAggregation();
            return namedAggregation != null ? new AggregationContainer {
                Aggregations = namedAggregation.Container.Aggregations
            } : new AggregationContainer();
        }

        public FilterContainer BuildFilter(string filter, IElasticQueryVisitorContext context = null) {
            var result = _parser.Parse(filter);
            return BuildFilter(result, context);
        }

        public FilterContainer BuildFilter(GroupNode filter, IElasticQueryVisitorContext context = null)  {
            var result = _parser.Parse(filter);

            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.SetGetFieldMappingFunc(_config.GetFieldMapping)
                .SetDefaultOperator(Operator.And)
                .SetDefaultField(_config.DefaultField);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);
            
            var filterNode = _config.FilterVisitor.Accept(result, context);

            return filterNode?.GetFilterContainer() ?? new MatchAllFilter();
        }
		
        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
        // automatic field alias management
    }
}

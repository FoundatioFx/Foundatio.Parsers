using System;
using System.Collections.Generic;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
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

        public FilterContainer BuildFilter(string filter, IQueryVisitorContext context = null)  {
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

        public QueryContainer BuildQuery(string query, IQueryVisitorContext context = null) {
            var result = _parser.Parse(query);

            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.SetGetFieldMappingFunc(_config.GetFieldMapping)
                .SetDefaultOperator(Operator.Or)
                .SetDefaultField(_config.DefaultField);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            var queryNode = _config.QueryVisitor.Accept(result, context);

            return queryNode?.GetQueryContainer() ?? new MatchAllQuery();
        }

        public AggregationContainer BuildAggregations(string aggregations, IQueryVisitorContext context = null) {
            var result = _parser.Parse(aggregations);

            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.SetGetFieldMappingFunc(_config.GetFieldMapping);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            var queryNode = _config.AggregationVisitor.Accept(result, context);

            var namedAggregation = queryNode?.GetAggregation();
            return namedAggregation != null ? new AggregationContainer {
                Aggregations = namedAggregation.Container.Aggregations
            } : new AggregationContainer();
        }

        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
        // automatic field alias management
    }
}

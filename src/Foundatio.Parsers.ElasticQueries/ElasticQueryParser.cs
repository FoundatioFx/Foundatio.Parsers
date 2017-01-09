using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Nest;
using Foundatio.Parsers.LuceneQueries.Nodes;
using System.Threading.Tasks;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParser {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();
        private readonly ElasticQueryParserConfiguration _config;

        public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);
            _config = config;
        }

        public Task<QueryContainer> BuildQueryAsync(string query, IElasticQueryVisitorContext context = null) {
            var result = _parser.Parse(query);
            return BuildQueryAsync(result, context);
        }

        public async Task<QueryContainer> BuildQueryAsync(GroupNode query, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            query.SetQueryType(QueryType.Query);
            context.SetGetPropertyMappingFunc(_config.GetMappingProperty)
                .SetDefaultField(_config.DefaultField);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            if (_config.DefaultIncludeResolver != null && context.GetIncludeResolver() == null)
                context.SetIncludeResolver(_config.DefaultIncludeResolver);

            if (_config.DefaultValidator != null && context.GetValidator() == null)
                context.SetValidator(_config.DefaultValidator);

            var queryNode = await _config.QueryVisitor.AcceptAsync(query, context).ConfigureAwait(false);

            var q = queryNode?.GetQuery() ?? new MatchAllQuery();
            if (!context.UseScoring) {
                q = new BoolQuery {
                    Filter = new QueryContainer[] { q }
                };
            }

            return q;
        }

        public Task<AggregationContainer> BuildAggregationsAsync(string aggregations, IElasticQueryVisitorContext context = null) {
            var result = _parser.Parse(aggregations);
            return BuildAggregationsAsync(result, context);
        }

        public async Task<AggregationContainer> BuildAggregationsAsync(GroupNode aggregations, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            aggregations.SetQueryType(QueryType.Aggregation);
            context.SetGetPropertyMappingFunc(_config.GetMappingProperty);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            if (_config.DefaultValidator != null && context.GetValidator() == null)
                context.SetValidator(_config.DefaultValidator);

            var queryNode = await _config.AggregationVisitor.AcceptAsync(aggregations, context).ConfigureAwait(false);

            return queryNode?.GetAggregation();
        }

        public Task<IEnumerable<IFieldSort>> BuildSortAsync(string sort, IElasticQueryVisitorContext context = null) {
            var result = _parser.Parse(sort);
            return BuildSortAsync(result, context);
        }

        public async Task<IEnumerable<IFieldSort>> BuildSortAsync(GroupNode sort, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            sort.SetQueryType(QueryType.Sort);
            context.SetGetPropertyMappingFunc(_config.GetMappingProperty);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            if (_config.DefaultValidator != null && context.GetValidator() == null)
                context.SetValidator(_config.DefaultValidator);

            var sortNode = await _config.SortVisitor.AcceptAsync(sort, context).ConfigureAwait(false);

            return await GetSortFieldsVisitor.RunAsync(sortNode, context).ConfigureAwait(false);
        }

        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
    }
}

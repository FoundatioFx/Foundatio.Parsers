using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Nest;
using Foundatio.Parsers.LuceneQueries.Nodes;
using System.Threading.Tasks;
using System.Collections.Generic;

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

            context.SetGetPropertyMappingFunc(_config.GetMappingProperty)
                .SetDefaultField(_config.DefaultField);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            if (_config.IncludeResolver != null && context.GetIncludeResolver() == null)
                context.SetIncludeResolver(_config.IncludeResolver);

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

            context.SetGetPropertyMappingFunc(_config.GetMappingProperty);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            var queryNode = await _config.AggregationVisitor.AcceptAsync(aggregations, context).ConfigureAwait(false);

            return queryNode?.GetAggregation();
        }

        public Task<IEnumerable<IFieldSort>> BuildSortAsync(string sort, IElasticQueryVisitorContext context = null) {
            var result = _parser.Parse(sort);
            return BuildSortAsync(result, context);
        }

        public async Task<IEnumerable<IFieldSort>> BuildSortAsync(GroupNode filter, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.SetGetPropertyMappingFunc(_config.GetMappingProperty);

            if (_config.DefaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_config.DefaultAliasResolver);

            var sortNode = await _config.SortVisitor.AcceptAsync(filter, context).ConfigureAwait(false);

            return await GetSortFieldsVisitor.RunAsync(sortNode, context).ConfigureAwait(false);
        }

        // parser query, generate filter, generate aggregations
        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
        // _exists_:field1
        // automatic field alias management
    }
}

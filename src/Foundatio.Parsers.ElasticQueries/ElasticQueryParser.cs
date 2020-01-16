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
    public class ElasticQueryParser : LuceneQueryParser {
        private readonly ElasticQueryParserConfiguration _config;

        public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);
            _config = config;
        }

        public ElasticQueryParserConfiguration Configuration => _config;

        public override async Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null) {
            if (String.IsNullOrEmpty(query))
                throw new ArgumentNullException(nameof(query));

            if (context == null)
                context = new ElasticQueryVisitorContext();

            var result = await base.ParseAsync(query, context).ConfigureAwait(false);
            SetupQueryVisitorContextDefaults(context);
            switch (context.QueryType) {
                case QueryType.Aggregation:
                    context.SetGetPropertyMappingFunc(_config.GetMappingProperty);
                    result = await _config.AggregationVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryType.Query:
                    context.SetGetPropertyMappingFunc(_config.GetMappingProperty).SetDefaultFields(_config.DefaultFields);
                    if (_config.IncludeResolver != null && context.GetIncludeResolver() == null)
                        context.SetIncludeResolver(_config.IncludeResolver);

                    result = await _config.QueryVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryType.Sort:
                    context.SetGetPropertyMappingFunc(_config.GetMappingProperty);
                    result = await _config.SortVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
            }

            return result;
        }

        private void SetupQueryVisitorContextDefaults(IQueryVisitorContext context) {
            if (_config.FieldResolver != null && context.GetFieldResolver() == null)
                context.SetFieldResolver(_config.FieldResolver);

            if (_config.Validator != null && context.GetValidator() == null)
                context.SetValidator(_config.Validator);
        }

        public async Task<QueryContainer> BuildQueryAsync(string query, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.QueryType = QueryType.Query;
            var result = await ParseAsync(query, context).ConfigureAwait(false);
            return await BuildQueryAsync(result, context).ConfigureAwait(false);
        }

        public Task<QueryContainer> BuildQueryAsync(IQueryNode query, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            var q = query.GetQuery() ?? new MatchAllQuery();
            if (context?.UseScoring == false) {
                q = new BoolQuery {
                    Filter = new QueryContainer[] { q }
                };
            }

            return Task.FromResult<QueryContainer>(q);
        }

        public async Task<AggregationContainer> BuildAggregationsAsync(string aggregations, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.QueryType = QueryType.Aggregation;
            var result = await ParseAsync(aggregations, context).ConfigureAwait(false);
            return await BuildAggregationsAsync(result, context).ConfigureAwait(false);
        }

        public Task<AggregationContainer> BuildAggregationsAsync(IQueryNode aggregations, IElasticQueryVisitorContext context = null) {
            return Task.FromResult<AggregationContainer>(aggregations?.GetAggregation());
        }

        public async Task<IEnumerable<IFieldSort>> BuildSortAsync(string sort, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.QueryType = QueryType.Sort;
            var result = await ParseAsync(sort, context).ConfigureAwait(false);
            return await BuildSortAsync(result, context).ConfigureAwait(false);
        }

        public Task<IEnumerable<IFieldSort>> BuildSortAsync(IQueryNode sort, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            return GetSortFieldsVisitor.RunAsync(sort, context);
        }

        // TODO: want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
    }
}

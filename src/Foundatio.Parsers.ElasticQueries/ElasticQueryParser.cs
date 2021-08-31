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
        public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);
            Configuration = config;
        }

        public ElasticQueryParserConfiguration Configuration { get; }

        public override async Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null) {
            if (String.IsNullOrEmpty(query))
                throw new ArgumentNullException(nameof(query));

            if (context == null)
                context = new ElasticQueryVisitorContext();

            var result = await base.ParseAsync(query, context).ConfigureAwait(false);
            SetupQueryVisitorContextDefaults(context);
            switch (context.QueryType) {
                case QueryType.Aggregation:
                    context.SetMappingResolver(Configuration.MappingResolver);
                    result = await Configuration.AggregationVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryType.Query:
                    context.SetMappingResolver(Configuration.MappingResolver).SetDefaultFields(Configuration.DefaultFields);
                    if (Configuration.IncludeResolver != null && context.GetIncludeResolver() == null)
                        context.SetIncludeResolver(Configuration.IncludeResolver);

                    result = await Configuration.QueryVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryType.Sort:
                    context.SetMappingResolver(Configuration.MappingResolver);
                    result = await Configuration.SortVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
            }

            return result;
        }

        private void SetupQueryVisitorContextDefaults(IQueryVisitorContext context) {
            if (Configuration.RuntimeFieldResolver != null && context.GetRuntimeFieldResolver() == null)
                context.SetRuntimeFieldResolver(Configuration.RuntimeFieldResolver);

            if (Configuration.FieldResolver != null && context.GetFieldResolver() == null)
                context.SetFieldResolver(Configuration.FieldResolver);

            if (Configuration.MappingResolver != null && context.GetMappingResolver() == null)
                context.SetMappingResolver(Configuration.MappingResolver);

            if (Configuration.ValidationOptions != null && context.GetValidationOptions() == null)
                context.SetValidationOptions(Configuration.ValidationOptions);
        }

        public async Task<QueryContainer> BuildQueryAsync(string query, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.QueryType = QueryType.Query;
            var result = await ParseAsync(query, context).ConfigureAwait(false);
            return await BuildQueryAsync(result, context).ConfigureAwait(false);
        }

        public async Task<QueryContainer> BuildQueryAsync(IQueryNode query, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            var q = await query.GetQueryAsync() ?? new MatchAllQuery();
            if (context?.UseScoring == false) {
                q = new BoolQuery {
                    Filter = new QueryContainer[] { q }
                };
            }

            return q;
        }

        public async Task<AggregationContainer> BuildAggregationsAsync(string aggregations, IElasticQueryVisitorContext context = null) {
            if (context == null)
                context = new ElasticQueryVisitorContext();

            context.QueryType = QueryType.Aggregation;
            var result = await ParseAsync(aggregations, context).ConfigureAwait(false);
            return await BuildAggregationsAsync(result, context).ConfigureAwait(false);
        }

#pragma warning disable IDE0060 // Remove unused parameter
        public async Task<AggregationContainer> BuildAggregationsAsync(IQueryNode aggregations, IElasticQueryVisitorContext context = null) {
            if (aggregations == null)
                return null;

#pragma warning restore IDE0060 // Remove unused parameter
            return await aggregations?.GetAggregationAsync();
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

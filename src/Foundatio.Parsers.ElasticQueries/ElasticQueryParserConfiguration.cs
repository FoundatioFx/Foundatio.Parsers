using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParserConfiguration {
        private ILogger _logger = NullLogger.Instance;

        public ElasticQueryParserConfiguration() {
            AddQueryVisitor(new CombineQueriesVisitor(), 10000);
            AddSortVisitor(new TermToFieldVisitor(), 0);
            AddAggregationVisitor(new AssignOperationTypeVisitor(), 0);
            AddAggregationVisitor(new CombineAggregationsVisitor(), 10000);
            AddVisitor(new FieldResolverQueryVisitor(), 10);
        }

        public ILoggerFactory LoggerFactory { get; private set; } = NullLoggerFactory.Instance;
        public string[] DefaultFields { get; private set; }
        public QueryFieldResolver FieldResolver { get; private set; }
        public IncludeResolver IncludeResolver { get; private set; }
        public ElasticMappingResolver MappingResolver { get; private set; }
        public QueryValidationOptions ValidationOptions { get; private set; }
        public ChainedQueryVisitor SortVisitor { get; } = new ChainedQueryVisitor();
        public ChainedQueryVisitor QueryVisitor { get; } = new ChainedQueryVisitor();
        public ChainedQueryVisitor AggregationVisitor { get; } = new ChainedQueryVisitor();

        public ElasticQueryParserConfiguration SetLoggerFactory(ILoggerFactory loggerFactory) {
            LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = loggerFactory.CreateLogger<ElasticQueryParserConfiguration>();
            
            return this;
        }

        public ElasticQueryParserConfiguration SetDefaultFields(string[] fields) {
            DefaultFields = fields;
            return this;
        }

        public ElasticQueryParserConfiguration UseFieldResolver(QueryFieldResolver resolver, int priority = 10) {
            FieldResolver = resolver;
            ReplaceVisitor<FieldResolverQueryVisitor>(new FieldResolverQueryVisitor(resolver), priority);

            return this;
        }

        public ElasticQueryParserConfiguration UseFieldMap(IDictionary<string, string> fields, int priority = 10) {
            if (fields != null)
                return UseFieldResolver(fields.ToHierarchicalFieldResolver(), priority);
            
            return UseFieldResolver(null);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, string> resolveGeoLocation, int priority = 200) {
            return UseGeo(location => Task.FromResult(resolveGeoLocation(location)), priority);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, Task<string>> resolveGeoLocation, int priority = 200) {
            return AddVisitor(new GeoVisitor(resolveGeoLocation), priority);
        }

        public ElasticQueryParserConfiguration UseIncludes(IncludeResolver includeResolver, ShouldSkipIncludeFunc shouldSkipInclude = null, int priority = 0) {
            IncludeResolver = includeResolver;

            return AddVisitor(new IncludeVisitor(shouldSkipInclude), priority);
        }

        public ElasticQueryParserConfiguration UseIncludes(Func<string, string> resolveInclude, ShouldSkipIncludeFunc shouldSkipInclude = null, int priority = 0) {
            return UseIncludes(name => Task.FromResult(resolveInclude(name)), shouldSkipInclude, priority);
        }

        public ElasticQueryParserConfiguration UseIncludes(IDictionary<string, string> includes, ShouldSkipIncludeFunc shouldSkipInclude = null, int priority = 0) {
            return UseIncludes(name => includes.ContainsKey(name) ? includes[name] : null, shouldSkipInclude, priority);
        }

        public ElasticQueryParserConfiguration UseValidation(QueryValidationOptions options, int priority = 0) {
            ValidationOptions = options;

            return AddVisitor(new ValidationVisitor { ShouldThrow = true }, priority);
        }

        public ElasticQueryParserConfiguration UseNested(int priority = 300) {
            return AddVisitor(new NestedVisitor(), priority);
        }

        #region Combined Visitor Management

        public ElasticQueryParserConfiguration AddVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            QueryVisitor.AddVisitor(visitor, priority);
            AggregationVisitor.AddVisitor(visitor, priority);
            SortVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveVisitor<T>() where T : IChainableQueryVisitor {
            QueryVisitor.RemoveVisitor<T>();
            AggregationVisitor.RemoveVisitor<T>();
            SortVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            QueryVisitor.ReplaceVisitor<T>(visitor, newPriority);
            AggregationVisitor.ReplaceVisitor<T>(visitor, newPriority);
            SortVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddVisitorBefore<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorBefore<T>(visitor);
            AggregationVisitor.AddVisitorBefore<T>(visitor);
            SortVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddVisitorAfter<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorAfter<T>(visitor);
            AggregationVisitor.AddVisitorAfter<T>(visitor);
            SortVisitor.AddVisitorAfter<T>(visitor);

            return this;
        }

        #endregion

        #region Query Visitor Management

        public ElasticQueryParserConfiguration AddQueryVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            QueryVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveQueryVisitor<T>() where T : IChainableQueryVisitor {
            QueryVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceQueryVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            QueryVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddQueryVisitorBefore<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddQueryVisitorAfter<T>(IChainableQueryVisitor visitor) {
            QueryVisitor.AddVisitorAfter<T>(visitor);

            return this;
        }

        #endregion

        #region Sort Visitor Management

        public ElasticQueryParserConfiguration AddSortVisitor(IChainableQueryVisitor visitor, int priority = 0)
        {
            SortVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveSortVisitor<T>() where T : IChainableQueryVisitor
        {
            SortVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceSortVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor
        {
            SortVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddSortVisitorBefore<T>(IChainableQueryVisitor visitor)
        {
            SortVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddSortVisitorAfter<T>(IChainableQueryVisitor visitor)
        {
            SortVisitor.AddVisitorAfter<T>(visitor);

            return this;
        }

        #endregion

        #region Aggregation Visitor Management

        public ElasticQueryParserConfiguration AddAggregationVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            AggregationVisitor.AddVisitor(visitor, priority);

            return this;
        }

        public ElasticQueryParserConfiguration RemoveAggregationVisitor<T>() where T : IChainableQueryVisitor {
            AggregationVisitor.RemoveVisitor<T>();

            return this;
        }

        public ElasticQueryParserConfiguration ReplaceAggregationVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            AggregationVisitor.ReplaceVisitor<T>(visitor, newPriority);

            return this;
        }

        public ElasticQueryParserConfiguration AddAggregationVisitorBefore<T>(IChainableQueryVisitor visitor) {
            AggregationVisitor.AddVisitorBefore<T>(visitor);

            return this;
        }

        public ElasticQueryParserConfiguration AddAggregationVisitorAfter<T>(IChainableQueryVisitor visitor) {
            AggregationVisitor.AddVisitorAfter<T>(visitor);

            return this;
        }

        #endregion

        public ElasticQueryParserConfiguration UseMappings<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> mappingBuilder, IElasticClient client, string index) where T : class {
            MappingResolver = ElasticMappingResolver.Create<T>(mappingBuilder, client, index, logger: _logger);

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> mappingBuilder, Inferrer inferrer, Func<ITypeMapping> getMapping) where T : class {
            MappingResolver = ElasticMappingResolver.Create<T>(mappingBuilder, inferrer, getMapping, logger: _logger);

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings<T>(IElasticClient client) {
            MappingResolver = ElasticMappingResolver.Create<T>(client, logger: _logger);

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings(IElasticClient client, string index) {
            MappingResolver = ElasticMappingResolver.Create(client, index, logger: _logger);

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings(Func<ITypeMapping> getMapping, Inferrer inferrer = null) {
            MappingResolver = ElasticMappingResolver.Create(getMapping, inferrer, logger: _logger);

            return this;
        }

        public ElasticQueryParserConfiguration UseMappings(ElasticMappingResolver resolver) {
            MappingResolver = resolver;

            return this;
        }
    }
}
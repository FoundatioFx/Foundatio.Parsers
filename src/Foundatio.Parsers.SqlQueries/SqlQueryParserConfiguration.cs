using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Visitors;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Parsers.SqlQueries;

public class SqlQueryParserConfiguration {
    private ILogger _logger = NullLogger.Instance;

    public SqlQueryParserConfiguration() {
        AddSortVisitor(new TermToFieldVisitor(), 0);
        AddVisitor(new FieldResolverQueryVisitor((field, context) => FieldResolver != null ? FieldResolver(field, context) : Task.FromResult<string>(null)), 10);
        AddVisitor(new ValidationVisitor(), 30);
    }

    public ILoggerFactory LoggerFactory { get; private set; } = NullLoggerFactory.Instance;
    public string[] DefaultFields { get; private set; }

    public QueryFieldResolver FieldResolver { get; private set; }
    public EntityTypeDynamicFieldsResolver EntityTypeDynamicFieldResolver { get; private set; }
    public EntityTypePropertyFilter EntityTypePropertyFilter { get; private set; } = static _ => true;
    public IncludeResolver IncludeResolver { get; private set; }
    //public ElasticMappingResolver MappingResolver { get; private set; }
    public QueryValidationOptions ValidationOptions { get; private set; }
    public ChainedQueryVisitor SortVisitor { get; } = new();
    public ChainedQueryVisitor QueryVisitor { get; } = new();
    public ChainedQueryVisitor AggregationVisitor { get; } = new();

    public SqlQueryParserConfiguration SetLoggerFactory(ILoggerFactory loggerFactory) {
        LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = loggerFactory.CreateLogger<SqlQueryParserConfiguration>();

        return this;
    }

    public SqlQueryParserConfiguration SetDefaultFields(string[] fields) {
        DefaultFields = fields;
        return this;
    }

    public SqlQueryParserConfiguration UseEntityTypeDynamicFieldResolver(EntityTypeDynamicFieldsResolver resolver) {
        EntityTypeDynamicFieldResolver = resolver;
        return this;
    }

    public SqlQueryParserConfiguration UseEntityTypePropertyFilter(EntityTypePropertyFilter filter) {
        EntityTypePropertyFilter = filter;
        return this;
    }

    public SqlQueryParserConfiguration UseFieldResolver(QueryFieldResolver resolver, int priority = 10) {
        FieldResolver = resolver;
        ReplaceVisitor<FieldResolverQueryVisitor>(new FieldResolverQueryVisitor(resolver), priority);

        return this;
    }

    public SqlQueryParserConfiguration UseFieldMap(IDictionary<string, string> fields, int priority = 10) {
        if (fields != null)
            return UseFieldResolver(fields.ToHierarchicalFieldResolver(), priority);

        return UseFieldResolver(null);
    }

    public SqlQueryParserConfiguration UseIncludes(IncludeResolver includeResolver, ShouldSkipIncludeFunc shouldSkipInclude = null, int priority = 0) {
        IncludeResolver = includeResolver;

        return AddVisitor(new IncludeVisitor(shouldSkipInclude), priority);
    }

    public SqlQueryParserConfiguration UseIncludes(Func<string, string> resolveInclude, ShouldSkipIncludeFunc shouldSkipInclude = null, int priority = 0) {
        return UseIncludes(name => Task.FromResult(resolveInclude(name)), shouldSkipInclude, priority);
    }

    public SqlQueryParserConfiguration UseIncludes(IDictionary<string, string> includes, ShouldSkipIncludeFunc shouldSkipInclude = null, int priority = 0) {
        return UseIncludes(name => includes.ContainsKey(name) ? includes[name] : null, shouldSkipInclude, priority);
    }

    public SqlQueryParserConfiguration SetValidationOptions(QueryValidationOptions options) {
        ValidationOptions = options;
        return this;
    }

    #region Combined Visitor Management

    public SqlQueryParserConfiguration AddVisitor(IChainableQueryVisitor visitor, int priority = 0) {
        QueryVisitor.AddVisitor(visitor, priority);
        AggregationVisitor.AddVisitor(visitor, priority);
        SortVisitor.AddVisitor(visitor, priority);

        return this;
    }

    public SqlQueryParserConfiguration RemoveVisitor<T>() where T : IChainableQueryVisitor {
        QueryVisitor.RemoveVisitor<T>();
        AggregationVisitor.RemoveVisitor<T>();
        SortVisitor.RemoveVisitor<T>();

        return this;
    }

    public SqlQueryParserConfiguration ReplaceVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
        QueryVisitor.ReplaceVisitor<T>(visitor, newPriority);
        AggregationVisitor.ReplaceVisitor<T>(visitor, newPriority);
        SortVisitor.ReplaceVisitor<T>(visitor, newPriority);

        return this;
    }

    public SqlQueryParserConfiguration AddVisitorBefore<T>(IChainableQueryVisitor visitor) {
        QueryVisitor.AddVisitorBefore<T>(visitor);
        AggregationVisitor.AddVisitorBefore<T>(visitor);
        SortVisitor.AddVisitorBefore<T>(visitor);

        return this;
    }

    public SqlQueryParserConfiguration AddVisitorAfter<T>(IChainableQueryVisitor visitor) {
        QueryVisitor.AddVisitorAfter<T>(visitor);
        AggregationVisitor.AddVisitorAfter<T>(visitor);
        SortVisitor.AddVisitorAfter<T>(visitor);

        return this;
    }

    #endregion

    #region Query Visitor Management

    public SqlQueryParserConfiguration AddQueryVisitor(IChainableQueryVisitor visitor, int priority = 0) {
        QueryVisitor.AddVisitor(visitor, priority);

        return this;
    }

    public SqlQueryParserConfiguration RemoveQueryVisitor<T>() where T : IChainableQueryVisitor {
        QueryVisitor.RemoveVisitor<T>();

        return this;
    }

    public SqlQueryParserConfiguration ReplaceQueryVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
        QueryVisitor.ReplaceVisitor<T>(visitor, newPriority);

        return this;
    }

    public SqlQueryParserConfiguration AddQueryVisitorBefore<T>(IChainableQueryVisitor visitor) {
        QueryVisitor.AddVisitorBefore<T>(visitor);

        return this;
    }

    public SqlQueryParserConfiguration AddQueryVisitorAfter<T>(IChainableQueryVisitor visitor) {
        QueryVisitor.AddVisitorAfter<T>(visitor);

        return this;
    }

    #endregion

    #region Sort Visitor Management

    public SqlQueryParserConfiguration AddSortVisitor(IChainableQueryVisitor visitor, int priority = 0) {
        SortVisitor.AddVisitor(visitor, priority);

        return this;
    }

    public SqlQueryParserConfiguration RemoveSortVisitor<T>() where T : IChainableQueryVisitor {
        SortVisitor.RemoveVisitor<T>();

        return this;
    }

    public SqlQueryParserConfiguration ReplaceSortVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
        SortVisitor.ReplaceVisitor<T>(visitor, newPriority);

        return this;
    }

    public SqlQueryParserConfiguration AddSortVisitorBefore<T>(IChainableQueryVisitor visitor) {
        SortVisitor.AddVisitorBefore<T>(visitor);

        return this;
    }

    public SqlQueryParserConfiguration AddSortVisitorAfter<T>(IChainableQueryVisitor visitor) {
        SortVisitor.AddVisitorAfter<T>(visitor);

        return this;
    }

    #endregion

    #region Aggregation Visitor Management

    public SqlQueryParserConfiguration AddAggregationVisitor(IChainableQueryVisitor visitor, int priority = 0) {
        AggregationVisitor.AddVisitor(visitor, priority);

        return this;
    }

    public SqlQueryParserConfiguration RemoveAggregationVisitor<T>() where T : IChainableQueryVisitor {
        AggregationVisitor.RemoveVisitor<T>();

        return this;
    }

    public SqlQueryParserConfiguration ReplaceAggregationVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
        AggregationVisitor.ReplaceVisitor<T>(visitor, newPriority);

        return this;
    }

    public SqlQueryParserConfiguration AddAggregationVisitorBefore<T>(IChainableQueryVisitor visitor) {
        AggregationVisitor.AddVisitorBefore<T>(visitor);

        return this;
    }

    public SqlQueryParserConfiguration AddAggregationVisitorAfter<T>(IChainableQueryVisitor visitor) {
        AggregationVisitor.AddVisitorAfter<T>(visitor);

        return this;
    }

    #endregion
}

public delegate Task<ICollection<EntityFieldInfo>> EntityTypeDynamicFieldsResolver(IEntityType entityType);
public delegate bool EntityTypePropertyFilter(IProperty property);

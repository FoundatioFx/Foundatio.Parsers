using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Linq.Dynamic.Core.CustomTypeProviders;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Extensions;
using Foundatio.Parsers.SqlQueries.Visitors;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Pegasus.Common;

namespace Foundatio.Parsers.SqlQueries;

public class SqlQueryParser : LuceneQueryParser
{
    public SqlQueryParser(Action<SqlQueryParserConfiguration>? configure = null)
    {
        var config = new SqlQueryParserConfiguration();
        configure?.Invoke(config);
        Configuration = config;
    }

    public SqlQueryParserConfiguration Configuration { get; }
    public ParsingConfig ParsingConfig { get; } = new()
    {
        CustomTypeProvider = new DynamicLinqTypeProvider()
    };

    public override async Task<IQueryNode?> ParseAsync(string query, IQueryVisitorContext? context = null)
    {
        query ??= String.Empty;
        context ??= new SqlQueryVisitorContext();

        SetupQueryVisitorContextDefaults(context);
        try
        {
            var result = await base.ParseAsync(query, context).AnyContext();
            if (result is null)
                return null;

            switch (context.QueryType)
            {
                case QueryTypes.Aggregation:
                    result = await Configuration.AggregationVisitor.AcceptAsync(result, context).AnyContext();
                    break;
                case QueryTypes.Query:
                    result = await Configuration.QueryVisitor.AcceptAsync(result, context).AnyContext();
                    break;
                case QueryTypes.Sort:
                    result = await Configuration.SortVisitor.AcceptAsync(result, context).AnyContext();
                    break;
            }

            return result;
        }
        catch (FormatException ex)
        {
            var cursor = ex.Data["cursor"] as Cursor;
            context.GetValidationResult().QueryType = context.QueryType;
            context.AddValidationError(ex.Message, cursor?.Column ?? 0);

            return null;
        }
    }

    private static readonly ConcurrentDictionary<IEntityType, List<EntityFieldInfo>> _entityFieldCache = new();
    public async Task<QueryValidationResult> ValidateAsync(string query, SqlQueryVisitorContext context)
    {
        var node = await ParseAsync(query, context).AnyContext();
        var validationResult = context.GetValidationResult();
        if (!validationResult.IsValid || node is null)
            return validationResult;

        return await ValidationVisitor.RunAsync(node, context).AnyContext();
    }

    public async Task<string> ToDynamicLinqAsync(string query, SqlQueryVisitorContext context)
    {
        var node = await ParseAsync(query, context).AnyContext();
        var result = context.GetValidationResult();
        if (!result.IsValid || node is null)
            throw new ValidationException("Invalid query: " + result.Message);

        return await GenerateSqlVisitor.RunAsync(node, context).AnyContext();
    }

    public SqlQueryVisitorContext GetContext(IEntityType entityType)
    {
        if (!_entityFieldCache.TryGetValue(entityType, out var fields))
        {
            fields = new List<EntityFieldInfo>();
            AddEntityFields(fields, null, entityType);
            _entityFieldCache.TryAdd(entityType, fields);
        }

        // make copy of fields list to avoid modifying the cached list
        fields = fields.ToList();

        var validationOptions = new QueryValidationOptions();
        foreach (string field in fields.Where(f => !f.IsNavigation && f.FullName is not null).Select(f => f.FullName!))
            validationOptions.AllowedFields.Add(field);

        Configuration.SetValidationOptions(validationOptions);
        return new SqlQueryVisitorContext
        {
            Fields = fields,
            ValidationOptions = validationOptions
        };
    }

    private void AddEntityFields(List<EntityFieldInfo> fields, EntityFieldInfo? parent, IEntityType entityType, Stack<IEntityType>? entityTypeStack = null, string? prefix = null, int depth = 0)
    {
        entityTypeStack ??= new Stack<IEntityType>();

        if (depth > 0 && entityTypeStack.Contains(entityType))
            return;

        entityTypeStack.Push(entityType);

        if (depth > Configuration.MaxFieldDepth)
            return;

        prefix ??= "";

        foreach (var property in entityType.GetProperties())
        {
            if (!Configuration.EntityTypePropertyFilter(property))
                continue;

            string propertyPath = prefix + property.Name;
            fields.Add(new EntityFieldInfo
            {
                Name = property.Name,
                FullName = propertyPath,
                IsNumber = property.ClrType.UnwrapNullable()?.IsNumeric() ?? false,
                IsDate = property.ClrType.UnwrapNullable()?.IsDateTime() ?? false,
                IsDateOnly = property.ClrType.UnwrapNullable()?.IsDateOnly() ?? false,
                IsBoolean = property.ClrType.UnwrapNullable()?.IsBoolean() ?? false,
                Parent = parent
            });
        }

        foreach (var nav in entityType.GetNavigations())
        {
            if (!Configuration.EntityTypeNavigationFilter(nav))
                continue;

            string propertyPath = prefix + nav.Name;
            bool isNavCollection = nav is IReadOnlyNavigationBase { IsCollection: true };

            var navFieldInfo = new EntityFieldInfo
            {
                IsCollection = isNavCollection,
                IsNavigation = true,
                Name = nav.Name,
                FullName = propertyPath,
                Parent = parent
            };
            fields.Add(navFieldInfo);

            AddEntityFields(fields, navFieldInfo, nav.TargetEntityType, entityTypeStack, propertyPath + ".", depth + 1);
        }

        foreach (var skipNav in entityType.GetSkipNavigations())
        {
            if (!Configuration.EntityTypeSkipNavigationFilter(skipNav))
                continue;

            string propertyPath = prefix + skipNav.Name;

            var navFieldInfo = new EntityFieldInfo
            {
                IsCollection = skipNav.IsCollection,
                IsNavigation = true,
                Name = skipNav.Name,
                FullName = propertyPath,
                Parent = parent
            };
            fields.Add(navFieldInfo);

            AddEntityFields(fields, navFieldInfo, skipNav.TargetEntityType, entityTypeStack, propertyPath + ".", depth + 1);
        }

        entityTypeStack.Pop();
    }

    private void SetupQueryVisitorContextDefaults(IQueryVisitorContext context)
    {
        if (!context.Data.ContainsKey("@OriginalContextResolver"))
            context.SetValue("@OriginalContextResolver", context.GetFieldResolver()!);

        context.SetFieldResolver(async (field, context) =>
        {
            string? resolvedField = null;
            if (context?.Data.TryGetValue("@OriginalContextResolver", out object? data) is true && data is QueryFieldResolver resolver)
            {
                string? contextResolvedField = await resolver(field, context).AnyContext();
                if (contextResolvedField != null)
                    resolvedField = contextResolvedField;
            }

            if (Configuration.FieldResolver != null)
            {
                string? configResolvedField = await Configuration.FieldResolver(resolvedField ?? field, context).AnyContext();
                if (configResolvedField != null)
                    resolvedField = configResolvedField;
            }

            return resolvedField;
        });

        if (Configuration.ValidationOptions != null && !context.HasValidationOptions())
            context.SetValidationOptions(Configuration.ValidationOptions);

        if (context.QueryType == QueryTypes.Query)
        {
            if (Configuration.DefaultFields is not null)
                context.SetDefaultFields(Configuration.DefaultFields);
            if (Configuration.IncludeResolver != null && context.GetIncludeResolver() == null)
                context.SetIncludeResolver(Configuration.IncludeResolver);
        }

        if (context is ISqlQueryVisitorContext sqlContext)
        {
            sqlContext.SearchTokenizer = Configuration.SearchTokenizer;
            sqlContext.DateTimeParser = Configuration.DateTimeParser;
            sqlContext.DateOnlyParser = Configuration.DateOnlyParser;
            sqlContext.DefaultSearchOperator = Configuration.DefaultFieldsSearchOperator;
            sqlContext.FullTextFields = Configuration.FullTextFields ?? [];
        }
    }
}

public static class FTS
{
    public static bool Contains(string propertyValue, string searchTerm)
    {
        return EF.Functions.Contains(propertyValue, searchTerm);
    }
}

public class DynamicLinqTypeProvider() : DefaultDynamicLinqCustomTypeProvider(ParsingConfig.Default, [typeof(EF), typeof(FTS)]);

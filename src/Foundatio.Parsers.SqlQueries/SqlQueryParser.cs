using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Extensions;
using Foundatio.Parsers.SqlQueries.Visitors;
using Microsoft.EntityFrameworkCore.Metadata;
using Pegasus.Common;

namespace Foundatio.Parsers.SqlQueries;

public class SqlQueryParser : LuceneQueryParser
{
    public SqlQueryParser(Action<SqlQueryParserConfiguration> configure = null)
    {
        var config = new SqlQueryParserConfiguration();
        configure?.Invoke(config);
        Configuration = config;
    }

    public SqlQueryParserConfiguration Configuration { get; }

    public override async Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null)
    {
        query ??= String.Empty;
        context ??= new SqlQueryVisitorContext();

        SetupQueryVisitorContextDefaults(context);
        try
        {
            var result = await base.ParseAsync(query, context).ConfigureAwait(false);
            switch (context.QueryType)
            {
                case QueryTypes.Aggregation:
                    result = await Configuration.AggregationVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryTypes.Query:
                    result = await Configuration.QueryVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryTypes.Sort:
                    result = await Configuration.SortVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
            }

            return result;
        }
        catch (FormatException ex)
        {
            var cursor = ex.Data["cursor"] as Cursor;
            context.GetValidationResult().QueryType = context.QueryType;
            context.AddValidationError(ex.Message, cursor.Column);

            return null;
        }
    }

    private static readonly ConcurrentDictionary<IEntityType, List<EntityFieldInfo>> _entityFieldCache = new();
    public async Task<QueryValidationResult> ValidateAsync(string query, SqlQueryVisitorContext context)
    {
        var node = await ParseAsync(query, context);
        return await ValidationVisitor.RunAsync(node, context);
    }

    public async Task<string> ToDynamicLinqAsync(string query, SqlQueryVisitorContext context)
    {
        var node = await ParseAsync(query, context);
        var result = context.GetValidationResult();
        if (!result.IsValid)
            throw new ValidationException("Invalid query: " + result.Message);

        return await GenerateSqlVisitor.RunAsync(node, context);
    }

    public SqlQueryVisitorContext GetContext(IEntityType entityType)
    {
        if (!_entityFieldCache.TryGetValue(entityType, out var fields))
        {
            fields = new List<EntityFieldInfo>();
            AddEntityFields(fields, entityType);
            _entityFieldCache.TryAdd(entityType, fields);
        }

        // make copy of fields list to avoid modifying the cached list
        fields = fields.ToList();

        var validationOptions = new QueryValidationOptions();
        foreach (string field in fields.Select(f => f.Field))
            validationOptions.AllowedFields.Add(field);

        Configuration.SetValidationOptions(validationOptions);
        return new SqlQueryVisitorContext
        {
            Fields = fields,
            ValidationOptions = validationOptions
        };
    }

    private void AddEntityFields(List<EntityFieldInfo> fields, IEntityType entityType, Stack<IEntityType> entityTypeStack = null, string prefix = null, bool isCollection = false, int depth = 0)
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
                Field = propertyPath,
                IsNumber = property.ClrType.UnwrapNullable().IsNumeric(),
                IsDate = property.ClrType.UnwrapNullable().IsDateTime(),
                IsBoolean = property.ClrType.UnwrapNullable().IsBoolean(),
                IsCollection = isCollection
            });
        }

        foreach (var nav in entityType.GetNavigations())
        {
            if (!Configuration.EntityTypeNavigationFilter(nav))
                continue;

            string propertyPath = prefix + nav.Name;
            bool isNavCollection = nav is IReadOnlyNavigationBase { IsCollection: true };

            AddEntityFields(fields, nav.TargetEntityType, entityTypeStack, propertyPath + ".", isNavCollection, depth + 1);
        }

        foreach (var skipNav in entityType.GetSkipNavigations())
        {
            if (!Configuration.EntityTypeSkipNavigationFilter(skipNav))
                continue;

            string propertyPath = prefix + skipNav.Name;

            AddEntityFields(fields, skipNav.TargetEntityType, entityTypeStack, propertyPath + ".", skipNav.IsCollection, depth + 1);
        }

        entityTypeStack.Pop();
    }

    private void SetupQueryVisitorContextDefaults(IQueryVisitorContext context)
    {
        if (!context.Data.ContainsKey("@OriginalContextResolver"))
            context.SetValue("@OriginalContextResolver", context.GetFieldResolver());

        context.SetFieldResolver(async (field, context) =>
        {
            string resolvedField = null;
            if (context.Data.TryGetValue("@OriginalContextResolver", out object data) && data is QueryFieldResolver resolver)
            {
                string contextResolvedField = await resolver(field, context).ConfigureAwait(false);
                if (contextResolvedField != null)
                    resolvedField = contextResolvedField;
            }

            if (Configuration.FieldResolver != null)
            {
                string configResolvedField = await Configuration.FieldResolver(resolvedField ?? field, context).ConfigureAwait(false);
                if (configResolvedField != null)
                    resolvedField = configResolvedField;
            }

            return resolvedField;
        });

        if (Configuration.ValidationOptions != null && !context.HasValidationOptions())
            context.SetValidationOptions(Configuration.ValidationOptions);

        if (context.QueryType == QueryTypes.Query)
        {
            context.SetDefaultFields(Configuration.DefaultFields);
            if (Configuration.IncludeResolver != null && context.GetIncludeResolver() == null)
                context.SetIncludeResolver(Configuration.IncludeResolver);
        }

        if (context is ISqlQueryVisitorContext sqlContext)
        {
            sqlContext.SearchTokenizer = Configuration.SearchTokenizer;
        }
    }
}

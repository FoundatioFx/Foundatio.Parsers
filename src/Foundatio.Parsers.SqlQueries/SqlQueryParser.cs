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

public class SqlQueryParser : LuceneQueryParser {
    public SqlQueryParser(Action<SqlQueryParserConfiguration> configure = null) {
        var config = new SqlQueryParserConfiguration();
        configure?.Invoke(config);
        Configuration = config;
    }

    public SqlQueryParserConfiguration Configuration { get; }

    public override async Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null) {
        query ??= String.Empty;
        context ??= new SqlQueryVisitorContext();

        SetupQueryVisitorContextDefaults(context);
        try {
            var result = await base.ParseAsync(query, context).ConfigureAwait(false);
            switch (context.QueryType) {
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
        } catch (FormatException ex) {
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

    public async Task<string> ToSqlAsync(string query, SqlQueryVisitorContext context)
    {
        var node = await ParseAsync(query, context);
        var result = await ValidationVisitor.RunAsync(node, context);
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

        var validationOptions = new QueryValidationOptions();
        foreach (string field in fields.Select(f => f.Field))
            validationOptions.AllowedFields.Add(field);

        Configuration.SetValidationOptions(validationOptions);
        return new SqlQueryVisitorContext
        {
            Fields = fields.ToList(),
            ValidationOptions = validationOptions
        };
    }

    private void AddEntityFields(List<EntityFieldInfo> fields, IEntityType entityType, List<IEntityType> visited = null, string prefix = null)
    {
        visited ??= [];
        if (visited.Contains(entityType))
            return;

        prefix ??= "";

        visited.Add(entityType);

        foreach (var property in entityType.GetProperties())
        {
            if (Configuration.EntityTypePropertyFilter(property))
                fields.Add(new EntityFieldInfo
                {
                    Field = prefix + property.Name,
                    IsNumber = property.ClrType.UnwrapNullable().IsNumeric(),
                    IsDate = property.ClrType.UnwrapNullable().IsDateTime(),
                    IsBoolean = property.ClrType.UnwrapNullable().IsBoolean()
                });
        }

        foreach (var nav in entityType.GetNavigations())
        {
            if (visited.Contains(nav.TargetEntityType))
                continue;

            AddEntityFields(fields, nav.TargetEntityType, visited, prefix + nav.Name + ".");
        }
    }

    private void SetupQueryVisitorContextDefaults(IQueryVisitorContext context)
    {
        if (!context.Data.ContainsKey("@OriginalContextResolver"))
            context.SetValue("@OriginalContextResolver", context.GetFieldResolver());

        context.SetFieldResolver(async (field, context) =>
        {
            string resolvedField = null;
            if (context.Data.TryGetValue("@OriginalContextResolver", out var data) && data is QueryFieldResolver resolver)
            {
                var contextResolvedField = await resolver(field, context).ConfigureAwait(false);
                if (contextResolvedField != null)
                    resolvedField = contextResolvedField;
            }

            if (Configuration.FieldResolver != null)
            {
                var configResolvedField = await Configuration.FieldResolver(resolvedField ?? field, context).ConfigureAwait(false);
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
    }
}

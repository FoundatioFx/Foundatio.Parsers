using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Linq.Dynamic.Core;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Extensions;
using Foundatio.Parsers.SqlQueries.Visitors;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Foundatio.Parsers.SqlQueries;

public static class QueryableExtensions
{
    private static readonly ConcurrentDictionary<IEntityType, List<FieldInfo>> _entityFieldCache = new();

    public static IQueryable<T> LuceneWhere<T>(this IQueryable<T> source, string query) where T : class
    {
        if (source is not DbSet<T> dbSet)
            throw new ArgumentException("source must be a DbSet<T>", nameof(source));

        var parser = dbSet.GetService<SqlQueryParser>();

        var fields = _entityFieldCache.GetOrAdd(dbSet.EntityType, entityType =>
        {
            var fields = new List<FieldInfo>();
            AddFields(fields, entityType);

            var dynamicFields = parser.Configuration.EntityTypeDynamicFieldResolver?.Invoke(entityType) ?? [];
            fields.AddRange(dynamicFields);

            return fields;
        });
        var validationOptions = new QueryValidationOptions();
        foreach (string field in fields.Select(f => f.Field))
            validationOptions.AllowedFields.Add(field);

        parser.Configuration.SetValidationOptions(validationOptions);
        var context = new SqlQueryVisitorContext { Fields = fields };
        var node = parser.Parse(query, context);
        var result = ValidationVisitor.Run(node, context);
        if (!result.IsValid)
            throw new ValidationException("Invalid query: " + result.Message);

        string sql = GenerateSqlVisitor.Run(node, context);
        return source.Where(sql);
    }

    private static void AddFields(List<FieldInfo> fields, IEntityType entityType, List<IEntityType> visited = null, string prefix = null)
    {
        visited ??= [];
        if (visited.Contains(entityType))
            return;

        prefix ??= "";

        visited.Add(entityType);

        foreach (var property in entityType.GetProperties())
        {
            if (property.IsIndex() || property.IsKey())
                fields.Add(new FieldInfo
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

            AddFields(fields, nav.TargetEntityType, visited, prefix + nav.Name + ".");
        }
    }
}

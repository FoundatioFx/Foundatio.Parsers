using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Reflection;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Extensions;
using Foundatio.Parsers.SqlQueries.Visitors;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using FieldInfo = Foundatio.Parsers.SqlQueries.Visitors.FieldInfo;

namespace Foundatio.Parsers.SqlQueries;

public static class QueryableExtensions
{
    private static readonly ConcurrentDictionary<IEntityType, List<FieldInfo>> _entityFieldCache = new();

    public static DbContext GetDbContext(IQueryable query)
    {
        var fi = query.GetType().GetField("_context", BindingFlags.NonPublic | BindingFlags.Instance);
        if (fi != null)
            return fi.GetValue(query) as DbContext;

        return null;
    }

    public static IQueryable<T> LuceneWhere<T>(this IQueryable<T> source, string query) where T : class
    {
        if (source is DbSet<T> dbSet)
        {
            return LuceneWhere(source, dbSet.EntityType, dbSet.GetService<SqlQueryParser>(), query);
        }

        var dbContext = GetDbContext(source);
        if (dbContext == null)
            throw new ArgumentException("Can't get DbContext from source", nameof(source));

        var entityType = dbContext.Model.FindEntityType(typeof(T));
        var parser = dbContext.GetService<SqlQueryParser>();

        return LuceneWhere(source, entityType, parser, query);
    }

    public static IQueryable<T> LuceneWhere<T>(this IQueryable<T> source, DbSet<T> dbSet, string query) where T : class
    {
        return LuceneWhere(source, dbSet.EntityType, dbSet.GetService<SqlQueryParser>(), query);
    }

    public static IQueryable<T> LuceneWhere<T>(this IQueryable<T> source, IEntityType entityType, SqlQueryParser parser, string query) where T : class
    {
        var fields = _entityFieldCache.GetOrAdd(entityType, e =>
        {
            var fields = new List<FieldInfo>();
            AddFields(fields, e);

            var dynamicFields = parser.Configuration.EntityTypeDynamicFieldResolver?.Invoke(e) ?? [];
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

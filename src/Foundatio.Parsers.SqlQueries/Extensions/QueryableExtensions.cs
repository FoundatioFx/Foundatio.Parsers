using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic.Core;
using Foundatio.Parsers.SqlQueries.Extensions;
using Foundatio.Parsers.SqlQueries.Visitors;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Foundatio.Parsers.SqlQueries;

public static class QueryableExtensions
{
    private static readonly SqlQueryParser _parser = new();
    private static readonly ConcurrentDictionary<IEntityType, List<FieldInfo>> _entityFieldCache = new();

    public static IQueryable<T> LuceneWhere<T>(this IQueryable<T> source, string query) where T : class
    {
        if (source is not DbSet<T> dbSet)
            throw new ArgumentException("source must be a DbSet<T>", nameof(source));

        var serviceProvider = ((IInfrastructure<IServiceProvider>)dbSet).Instance;
        var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
        var logger = loggerFactory.CreateLogger<SqlQueryParser>();

        // use service provider to get global settings that say how to discover and handle custom fields
        // support field aliases

        var fields = _entityFieldCache.GetOrAdd(dbSet.EntityType, entityType =>
        {
            var fields = new List<FieldInfo>();
            AddFields(fields, entityType);

            // lookup and add custom fields
            fields.Add(new FieldInfo
            {
                Field = "age",
                Data = {{ "DataDefinitionId", 1 }},
                IsNumber = true
            });

            return fields;
        });

        var context = new SqlQueryVisitorContext { Fields = fields };
        var node = _parser.Parse(query, context);
        string sql = GenerateSqlVisitor.Run(node, context);
        return source.Where(sql);
    }

    private static void AddFields(List<FieldInfo> fields, IEntityType entityType, List<IEntityType> visited = null)
    {
        visited ??= [];
        if (visited.Contains(entityType))
            return;

        visited.Add(entityType);

        foreach (var property in entityType.GetProperties())
        {
            if (property.IsIndex() || property.IsKey())
                fields.Add(new FieldInfo
                {
                    Field = property.Name,
                    IsNumber = property.ClrType.UnwrapNullable().IsNumeric(),
                    IsDate = property.ClrType.UnwrapNullable().IsDateTime(),
                    IsBoolean = property.ClrType.UnwrapNullable().IsBoolean()
                });
        }

        foreach (var nav in entityType.GetNavigations())
        {
            if (visited.Contains(nav.TargetEntityType))
                continue;

            var field = new FieldInfo
            {
                Field = nav.Name,
                Children = new List<FieldInfo>()
            };
            fields.Add(field);
            AddFields(field.Children, nav.TargetEntityType, visited);
        }
    }
}

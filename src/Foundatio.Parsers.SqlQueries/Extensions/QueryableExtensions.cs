using System;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Foundatio.Parsers.SqlQueries;

public static class QueryableExtensions
{
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
            return LuceneWhere(source, query, dbSet.EntityType, dbSet.GetService<SqlQueryParser>());
        }

        var dbContext = GetDbContext(source);
        if (dbContext == null)
            throw new ArgumentException("Can't get DbContext from source", nameof(source));

        var entityType = dbContext.Model.FindEntityType(typeof(T));
        var parser = dbContext.GetService<SqlQueryParser>();

        return LuceneWhere(source, query, entityType, parser);
    }

    public static IQueryable<T> LuceneWhere<T>(this IQueryable<T> source, string query, DbSet<T> dbSet) where T : class
    {
        return LuceneWhere(source, query, dbSet.EntityType, dbSet.GetService<SqlQueryParser>());
    }

    public static IQueryable<T> LuceneWhere<T>(this IQueryable<T> source, string query, IEntityType entityType, SqlQueryParser parser) where T : class
    {
        return source.Where(parser.ToDynamicSql(query, entityType));
    }
}

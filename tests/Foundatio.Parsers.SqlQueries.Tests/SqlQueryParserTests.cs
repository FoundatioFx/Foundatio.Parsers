using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Text;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Extensions;
using Foundatio.Parsers.SqlQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Pegasus.Common.Tracing;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.SqlQueries.Tests;

public class SqlQueryParserTests : TestWithLoggingBase {
    public SqlQueryParserTests(ITestOutputHelper output) : base(output) {
        Log.MinimumLevel = LogLevel.Trace;
    }

    [Theory]
    [InlineData("field:[1 TO 5]", "(field >= 1 AND field <= 5)")]
    [InlineData("field:{1 TO 5}", "(field > 1 AND field < 5)")]
    [InlineData("field:[1 TO 5}", "(field >= 1 AND field < 5)")]
    [InlineData("field:>5", "field > 5")]
    [InlineData("field:>=5", "field >= 5")]
    [InlineData("field:<5", "field < 5")]
    [InlineData("field:<=5", "field <= 5")]
    // [InlineData("date:>now")]
    // [InlineData("date:<now")]
    // [InlineData("_exists_:title")]
    // [InlineData("_missing_:title")]
    // [InlineData("date:[now/d-4d TO now/d+1d}")]
    // [InlineData("(date:[now/d-4d TO now/d+1d})")]
    // [InlineData("data.date:>now")]
    // [InlineData("data.date:[now/d-4d TO now/d+1d}")]
    // [InlineData("data.date:[2012-01-01 TO 2012-12-31]")]
    // [InlineData("data.date:[* TO 2012-12-31]")]
    // [InlineData("data.date:[2012-01-01 TO *]")]
    // [InlineData("(data.date:[now/d-4d TO now/d+1d})")]
    // [InlineData("count:>1")]
    // [InlineData("count:>=1")]
    // [InlineData("count:[1..5}")]
    // [InlineData(@"count:a\:a")]
    // [InlineData(@"count:a\:a more:stuff")]
    // [InlineData("data.count:[1..5}")]
    // [InlineData("age:[1 TO 2]")]
    // [InlineData("data.Windows-identity:ejsmith")]
    // [InlineData("data.age:[* TO 10]")]
    // [InlineData("hidden:true")]
    public Task ValidQueries(string query, string expected)
    {
        return ParseAndValidateQuery(query, expected, true);
    }

    [Fact]
    public async Task CanGenerateSql()
    {
        var services = new ServiceCollection();
        services.AddDbContext<SampleContext>((_, x) =>
        {
            x.UseSqlServer("Server=localhost;User Id=sa;Password=P@ssword1;Timeout=5;Initial Catalog=foundatio;Encrypt=False", o => o.MigrationsAssembly("ConcordServicing.Web"));
        }, ServiceLifetime.Scoped, ServiceLifetime.Singleton);
        var parser = new SqlQueryParser();
        parser.Configuration.AddQueryVisitor(new DynamicFieldVisitor());
        parser.Configuration.UseEntityTypeDynamicFieldResolver(async entityType =>
        {
            var dynamicFields = new List<EntityFieldInfo>();
            if (entityType.ClrType == typeof(Employee))
            {
                dynamicFields.Add(new EntityFieldInfo { Field = "age", IsNumber = true, Data = {{ "DataDefinitionId", 1 }}});
            }
            return dynamicFields;
        });
        parser.Configuration.UseEntityTypePropertyFilter(p =>
        {
            if (p.DeclaringType.ClrType == typeof(Company) && p.Name == "Description")
                return false;

            return true;
        });
        services.AddSingleton(parser);
        var sp = services.BuildServiceProvider();

        await using var db = sp.GetRequiredService<SampleContext>();

        var dbParser = db.GetService<SqlQueryParser>();
        Assert.Same(parser, dbParser);
        var dbSetParser = db.Employees.GetService<SqlQueryParser>();
        Assert.Same(parser, dbSetParser);

        await db.Database.EnsureDeletedAsync();
        await db.Database.EnsureCreatedAsync();

        var company = new Company {
            Name = "Acme",
            DataDefinitions = [ new() { Key = "age", DataType = DataType.Number } ]
        };
        db.Companies.Add(company);
        db.Employees.Add(new Employee
        {
            FullName = "John Doe",
            Title = "Software Developer",
            DataValues = [ new() { Definition = company.DataDefinitions[0], NumberValue = 30 } ],
            Company = company
        });
        db.Employees.Add(new Employee
        {
            FullName = "Jane Doe",
            Title = "Software Developer",
            DataValues = [ new() { Definition = company.DataDefinitions[0], NumberValue = 23 } ],
            Company = company
        });
        await db.SaveChangesAsync();

        string sqlExpected = db.Employees.Where(e => e.Company.Name == "acme" && e.DataValues.Any(dv => dv.DataDefinitionId == 1 && dv.NumberValue == 30)).ToQueryString();
        string sqlActual = db.Employees.Where("""company.name = "acme" AND DataValues.Any(DataDefinitionId = 1 AND NumberValue = 30) """).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToSqlAsync("company.name:acme age:30", db.Employees.EntityType);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        var q = db.Employees.AsNoTracking();
        sql = await parser.ToSqlAsync("company.name:acme age:30", db.Employees.EntityType);
        sqlActual = q.Where(sql, db.Employees).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        await Assert.ThrowsAsync<ValidationException>(() => parser.ToSqlAsync("company.description:acme", db.Employees.EntityType));

        var employees = await db.Employees.Where(e => e.Title == "software developer" && e.DataValues.Any(dv => dv.DataDefinitionId == 1 && dv.NumberValue == 30))
            .ToListAsync();

        Assert.Single(employees);
        var employee = employees.Single();
        Assert.Equal("John Doe", employee.FullName);
    }

    private async Task ParseAndValidateQuery(string query, string expected, bool isValid)
    {
#if ENABLE_TRACING
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
        var tracer = NullTracer.Instance;
#endif
        var parser = new SqlQueryParser
        {
            Tracer = tracer
        };

        IQueryNode result;
        try
        {
            result = await parser.ParseAsync(query);
        }
        catch (FormatException ex)
        {
            Assert.False(isValid, ex.Message);
            return;
        }

        string nodes = await DebugQueryVisitor.RunAsync(result);
        _logger.LogInformation(nodes);
        string generatedQuery = await GenerateSqlVisitor.RunAsync(result, new SqlQueryVisitorContext());
        Assert.Equal(expected, generatedQuery);
    }
}

public class DynamicFieldVisitor : ChainableMutatingQueryVisitor
{
    public override IQueryNode Visit(TermNode node, IQueryVisitorContext context)
    {
        if (context is not SqlQueryVisitorContext sqlContext)
            return node;

        var field = SqlNodeExtensions.GetFieldInfo(sqlContext.Fields, node.Field);

        if (field == null || !field.Data.TryGetValue("DataDefinitionId", out object value) ||
            value is not int dataDefinitionId)
        {
            return node;
        }

        var customFieldBuilder = new StringBuilder();

        customFieldBuilder.Append("DataValues.Any(DataDefinitionId = ");
        customFieldBuilder.Append(dataDefinitionId);
        customFieldBuilder.Append(" AND ");
        switch (field)
        {
            case { IsNumber: true }:
                customFieldBuilder.Append("NumberValue");
                break;
            case { IsBoolean: true }:
                customFieldBuilder.Append("BooleanValue");
                break;
            case { IsDate: true }:
                customFieldBuilder.Append("DateValue");
                break;
            default:
                customFieldBuilder.Append("StringValue");
                break;
        }

        customFieldBuilder.Append(" = ");
        if (field is { IsNumber: true } or { IsBoolean: true })
        {
            customFieldBuilder.Append(node.Term);
        }
        else
        {
            customFieldBuilder.Append("\"");
            customFieldBuilder.Append(node.Term);
            customFieldBuilder.Append("\"");
        }
        customFieldBuilder.Append(")");

        node.SetQuery(customFieldBuilder.ToString());

        return node;
    }
}

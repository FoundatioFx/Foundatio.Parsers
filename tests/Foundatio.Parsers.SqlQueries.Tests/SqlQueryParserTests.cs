using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
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

public class SqlQueryParserTests : TestWithLoggingBase
{
    public SqlQueryParserTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultMinimumLevel = LogLevel.Trace;
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
    public async Task CanSearchDefaultFields()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();
        parser.Configuration.SetDefaultFields(["FullName", "Title"]);

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.FullName == "John Doe" || e.Title == "John Doe").ToQueryString();
        string sqlActual = db.Employees.Where("""FullName = "John Doe" || Title = "John Doe" """).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("John Doe", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        var results = await db.Employees.Where(sql).ToListAsync();
        Assert.Single(results);
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanSearchWithTokenizer()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();
        parser.Configuration.SetDefaultFields(["SearchValues.Term"]);
        parser.Configuration.SetTokenizer(t =>
        {
            string[] terms = [t.Replace("-", "")];
            return terms.Distinct().ToArray();
        });

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.SearchValues.Any(s => s.Term == "2142222222")).ToQueryString();
        string sqlActual = db.Employees.Where("""SearchValues.Any(Term in ("2142222222"))""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("214-222-2222", context);
        _logger.LogInformation(sql);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        var results = await db.Employees.Where(sql).ToListAsync();
        Assert.Single(results);
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanUseDateFilter()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.Created > new DateTime(2024, 1, 1)).ToQueryString();
        string sqlActual = db.Employees.Where("""created > DateTime.Parse("2024-01-01")""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("created:>2024-01-01", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanUseExistsFilter()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.Title != null).ToQueryString();
        string sqlActual = db.Employees.Where("""Title != null""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("_exists_:title", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanUseMissingFilter()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.Title == null).ToQueryString();
        string sqlActual = db.Employees.Where("""Title == null""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("_missing_:title", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanUseDateMathFilter()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.Created > DateTime.UtcNow.AddDays(-90)).ToQueryString();
        string sqlActual = db.Employees.Where("""created > DateTime.UtcNow.AddDays(-90)""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("created:>now-90d", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanUseCollectionDefaultFields()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();
        parser.Configuration.SetDefaultFields(["Companies.Name"]);

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.Companies.Any(c => c.Name.Contains("acme"))).ToQueryString();
        string sqlActual = db.Employees.Where("""Companies.Any(Name.Contains("acme"))""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("acme", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanUseNavigationFields()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Companies.EntityType);

        Assert.Contains(db.Companies.EntityType.GetNavigations(), e => e.TargetEntityType == db.DataDefinitions.EntityType);

        string sqlExpected = db.Companies.Where(e => e.DataDefinitions.Any(c => c.Key == "age")).ToQueryString();
        string sqlActual = db.Companies.Where("""DataDefinitions.Any(Key.Equals("age"))""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("datadefinitions.key:age", context);
        sqlActual = db.Companies.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        var query = db.Companies.AsQueryable();
        var companies = await query.Where(sql).ToListAsync();

        Assert.Single(companies);
    }

    [Fact]
    public async Task CanUseSkipNavigationFields()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Companies.EntityType);

        Assert.Contains(db.Companies.EntityType.GetSkipNavigations(), e => e.TargetEntityType == db.Employees.EntityType);

        string sqlExpected = db.Companies.Where(e => e.Employees.Any(c => c.Salary.Equals(80_000))).ToQueryString();
        string sqlActual = db.Companies.Where("""Employees.Any(Salary.Equals(80000))""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("employees.salary:80000", context);
        sqlActual = db.Companies.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        var query = db.Companies.AsQueryable();
        var companies = await query.Where(sql).ToListAsync();

        Assert.Single(companies);
    }

    [Fact]
    public async Task CanGenerateSql()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Employees.EntityType);
        context.Fields.Add(new EntityFieldInfo { Field = "age", IsNumber = true, Data = { { "DataDefinitionId", 1 } } });
        context.ValidationOptions.AllowedFields.Add("age");

        string sqlExpected = db.Employees.Where(e => e.Companies.Any(c => c.Name == "acme") && e.DataValues.Any(dv => dv.DataDefinitionId == 1 && dv.NumberValue == 30)).ToQueryString();
        string sqlActual = db.Employees.Where("""Companies.Any(Name = "acme") AND DataValues.Any(DataDefinitionId = 1 AND NumberValue = 30) """).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("companies.name:acme age:30", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        var q = db.Employees.AsNoTracking();
        sql = await parser.ToDynamicLinqAsync("companies.name:acme age:30", context);
        sqlActual = q.Where(sql, db.Employees).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        await Assert.ThrowsAsync<ValidationException>(() => parser.ToDynamicLinqAsync("companies.description:acme", context));

        var employees = await db.Employees.Where(e => e.Title == "software developer" && e.DataValues.Any(dv => dv.DataDefinitionId == 1 && dv.NumberValue == 30))
            .ToListAsync();

        Assert.Single(employees);
        var employee = employees.Single();
        Assert.Equal("John Doe", employee.FullName);
    }

    public IServiceProvider GetServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddDbContext<SampleContext>((_, x) =>
        {
            x.UseSqlServer("Server=localhost;User Id=sa;Password=P@ssword1;Timeout=5;Initial Catalog=foundatio;Encrypt=False");
        }, ServiceLifetime.Scoped, ServiceLifetime.Singleton);
        var parser = new SqlQueryParser();
        parser.Configuration.UseEntityTypePropertyFilter(p => p.Name != nameof(Company.Description));
        parser.Configuration.AddQueryVisitor(new DynamicFieldVisitor());
        services.AddSingleton(parser);
        return services.BuildServiceProvider();
    }

    public async Task<SampleContext> GetSampleContextWithDataAsync(IServiceProvider sp)
    {
        var db = sp.GetRequiredService<SampleContext>();
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var dbParser = db.GetService<SqlQueryParser>();
        Assert.Same(parser, dbParser);
        var dbSetParser = db.Employees.GetService<SqlQueryParser>();
        Assert.Same(parser, dbSetParser);

        await db.Database.EnsureDeletedAsync();
        await db.Database.EnsureCreatedAsync();

        var company = new Company
        {
            Name = "Acme",
            DataDefinitions = [new() { Key = "age", DataType = DataType.Number }]
        };
        db.Companies.Add(company);
        db.Employees.Add(new Employee
        {
            FullName = "John Doe",
            Title = "Software Developer",
            PhoneNumber = "(214) 222-2222",
            Salary = 80_000,
            DataValues = [new() { Definition = company.DataDefinitions[0], NumberValue = 30 }],
            Companies = [company],
            SearchValues = [
                new() { Term = "john" },
                new() { Term = "doe" },
                new() { Term = "software" },
                new() { Term = "developer" },
                new() { Term = "2142222222" }
            ]
        });
        db.Employees.Add(new Employee
        {
            FullName = "Jane Doe",
            Title = "Software Developer",
            Salary = 90_000,
            DataValues = [new() { Definition = company.DataDefinitions[0], NumberValue = 23 }],
            Companies = [company]
        });
        await db.SaveChangesAsync();

        return db;
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
        _logger.LogInformation("{Nodes}", nodes);
        var context = new SqlQueryVisitorContext
        {
            Fields =
            [
                new EntityFieldInfo { Field = "field", IsNumber = true }
            ]
        };
        string generatedQuery = await GenerateSqlVisitor.RunAsync(result, context);
        Assert.Equal(expected, generatedQuery);
    }
}

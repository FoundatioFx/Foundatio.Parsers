using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Pegasus.Common.Tracing;
using PhoneNumbers;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.SqlQueries.Tests;

public class SqlQueryParserTests : TestWithLoggingBase
{
    public SqlQueryParserTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = LogLevel.Trace;
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

        string sqlExpected = db.Employees.Where(e => EF.Functions.Contains(e.FullName, "\"John*\"") || EF.Functions.Contains(e.Title, "\"John*\"")).ToQueryString();
        string sqlActual = db.Employees.Where(parser.ParsingConfig, """FTS.Contains(FullName, "\"John*\"") || FTS.Contains(Title, "\"John*\"") """).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("John", context);
        sqlActual = db.Employees.Where(parser.ParsingConfig, sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        await SqlWaiter.WaitForFullTextIndexAsync(db, "ftCatalog");

        var results = await db.Employees.Where(parser.ParsingConfig, sql).ToListAsync();
        Assert.Single(results);
    }

    [Fact]
    public async Task CanSearchWithTokenizer()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();
        parser.Configuration.SetDefaultFields(["NationalPhoneNumber"]);
        parser.Configuration.SetSearchTokenizer(s =>
        {
            if (String.IsNullOrWhiteSpace(s.Term))
                return;

            if (s.FieldInfo.FullName != "NationalPhoneNumber")
                return;

            s.Tokens = [TryGetNationalNumber(s.Term)];
            s.Operator = SqlSearchOperator.StartsWith;
        });

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => EF.Functions.Contains(e.NationalPhoneNumber, "\"2142222222*\"")).ToQueryString();
        string sqlActual = db.Employees.Where(parser.ParsingConfig, "FTS.Contains(NationalPhoneNumber, \"\\\"2142222222*\\\"\")").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        string sql = await parser.ToDynamicLinqAsync("214-222-2222", context);
        _logger.LogInformation(sql);
        Assert.Equal(sqlExpected, sqlActual);

        await SqlWaiter.WaitForFullTextIndexAsync(db, "ftCatalog");

        sqlActual = db.Employees.Where(parser.ParsingConfig, sql).ToQueryString();
        var results = await db.Employees.Where(parser.ParsingConfig, sql).ToListAsync();
        Assert.Single(results);

        sql = await parser.ToDynamicLinqAsync("2142222222", context);
        _logger.LogInformation(sql);
        sqlActual = db.Employees.Where(parser.ParsingConfig, sql).ToQueryString();
        results = await db.Employees.Where(parser.ParsingConfig, sql).ToListAsync();
        Assert.Single(results);
        Assert.Equal(sqlExpected, sqlActual);

        sql = await parser.ToDynamicLinqAsync("21422", context);
        _logger.LogInformation(sql);
        sqlActual = db.Employees.Where(parser.ParsingConfig, sql).ToQueryString();
        results = await db.Employees.Where(parser.ParsingConfig, sql).ToListAsync();
        Assert.Single(results);
    }

    [Fact]
    public async Task CanHandleEmptyTokens()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();
        parser.Configuration.SetDefaultFields(["NationalPhoneNumber"]);
        parser.Configuration.SetSearchTokenizer(s =>
        {
            s.Tokens = ["", "    "];
        });

        var context = parser.GetContext(db.Employees.EntityType);

        string sql = await parser.ToDynamicLinqAsync("test", context);
        _logger.LogInformation(sql);
        string sqlActual = db.Employees.Where(parser.ParsingConfig, sql).ToQueryString();
        var results = await db.Employees.Where(parser.ParsingConfig, sql).ToListAsync();
        Assert.Empty(results);
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
    public async Task CanUseDateParser()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();
        var tz = TimeZoneInfo.FindSystemTimeZoneById("Asia/Tokyo");
        var utcNow = DateTime.UtcNow;
        _logger.LogInformation("UtcNow: {UtcNow:O} {UtcTicks}", utcNow, utcNow.Ticks);
        var localNow = TimeZoneInfo.ConvertTimeFromUtc(utcNow, tz);
        _logger.LogInformation("LocalNow: {LocalNow:O}", localNow);

        parser.Configuration.DateTimeParser = dateTimeValue =>
        {
            if (dateTimeValue.Equals("now", StringComparison.OrdinalIgnoreCase))
                return "DateTime.UtcNow";

            var dateTime = DateTime.Parse(dateTimeValue);
            _logger.LogInformation("Parsed DateTime: {DateTime:O} {DateTimeKind}", dateTime, dateTime.Kind);

            if (dateTime.Kind != DateTimeKind.Utc)
                dateTime = TimeZoneInfo.ConvertTimeToUtc(dateTime, tz);

            _logger.LogInformation("Parsed UTC DateTime: {DateTime:O} {UtcTicks}", dateTime, dateTime.Ticks);
            return "DateTime(" + dateTime.Ticks + ", DateTimeKind.Utc)";
        };

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlActual = db.Employees.Where($"created > DateTime({utcNow.Ticks}, DateTimeKind.Utc)").ToQueryString();
        Assert.Contains(utcNow.ToString("O"), sqlActual);
        string sql = await parser.ToDynamicLinqAsync($"created:>\"{localNow:O}\"", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Contains(utcNow.ToString("O"), sqlActual);
    }

    [Fact]
    public async Task CanUseDateOnlyFilter()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.Birthday < DateOnly.FromDateTime(DateTime.UtcNow).AddDays(-90)).ToQueryString();
        string sqlActual = db.Employees.Where("""birthday < DateOnly.FromDateTime(DateTime.UtcNow).AddDays(-90)""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("birthday:<now-90d", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);

        sqlExpected = db.Employees.Where(e => e.Birthday == DateOnly.FromDateTime(DateTime.UtcNow).AddDays(-90)).ToQueryString();
        sqlActual = db.Employees.Where("""birthday = DateOnly.FromDateTime(DateTime.UtcNow).AddDays(-90)""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        sql = await parser.ToDynamicLinqAsync("birthday:now-90d", context);
        sqlActual = db.Employees.Where(sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanUseTimeOnlyFilter()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.HappyHour < TimeOnly.Parse("6:00")).ToQueryString();
        string sqlActual = db.Employees.Where("""happyhour < TimeOnly.Parse("6:00")""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("""happyhour:<"6:00" """, context);
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

        string sqlExpected = db.Employees.Where(e => e.Companies.Any(c => EF.Functions.Contains(c.Name, "\"acme*\""))).ToQueryString();
        string sqlActual = db.Employees.Where(parser.ParsingConfig, """Companies.Any(FTS.Contains(Name, "\"acme*\""))""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("acme", context);
        sqlActual = db.Employees.Where(parser.ParsingConfig, sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanUseCollectionDefaultFieldsWithNestedDepth()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();
        parser.Configuration.SetDefaultFields(["Companies.DataDefinitions.Key"]);

        var context = parser.GetContext(db.Employees.EntityType);

        string sqlExpected = db.Employees.Where(e => e.Companies.Any(c => c.DataDefinitions.Any(e => e.Key.StartsWith("age")))).ToQueryString();
        string sqlActual = db.Employees.Where("""Companies.Any(DataDefinitions.Any(Key.StartsWith("age")))""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("age", context);
        _logger.LogInformation(sql);
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
    public async Task CanUseLike()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Employees.EntityType);
        string sqlExpected = db.Employees.Where(e => EF.Functions.Contains(e.FullName, "john")).ToQueryString();
        string sqlActual = db.Employees.Where(parser.ParsingConfig, """FTS.Contains(FullName, "john")""").ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
        string sql = await parser.ToDynamicLinqAsync("john", context);
        sqlActual = db.Employees.Where(parser.ParsingConfig, sql).ToQueryString();
        Assert.Equal(sqlExpected, sqlActual);
    }

    [Fact]
    public async Task CanGenerateSql()
    {
        var sp = GetServiceProvider();
        await using var db = await GetSampleContextWithDataAsync(sp);
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var context = parser.GetContext(db.Employees.EntityType);
        context.Fields.Add(new EntityFieldInfo { Name = "age", FullName = "age", IsNumber = true, Data = { { "DataDefinitionId", 1 } } });
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

    public static string TryGetNationalNumber(string phoneNumber, string regionCode = "US")
    {
        var phoneNumberUtil = PhoneNumberUtil.GetInstance();
        try
        {
            return phoneNumberUtil.Parse(phoneNumber, regionCode).NationalNumber.ToString();
        }
        catch (NumberParseException)
        {
            return null;
        }
    }

    public IServiceProvider GetServiceProvider()
    {
        string sqlConnectionString = "Server=localhost;User Id=sa;Password=P@ssword1;Timeout=5;Initial Catalog=foundatio;Encrypt=False";
        SqlWaiter.Wait(sqlConnectionString);

        var services = new ServiceCollection();
        services.AddDbContext<SampleContext>((_, x) =>
        {
            x.UseSqlServer("Server=localhost;User Id=sa;Password=P@ssword1;Timeout=5;Initial Catalog=foundatio;Encrypt=False");
        }, ServiceLifetime.Scoped, ServiceLifetime.Singleton);
        var parser = new SqlQueryParser();
        parser.Configuration.UseEntityTypePropertyFilter(p => p.Name != nameof(Company.Description));
        parser.Configuration.AddQueryVisitor(new DynamicFieldVisitor());
        parser.Configuration.SetDefaultFields(["FullName"], SqlSearchOperator.Contains);
        parser.Configuration.SetFullTextFields(["Name", "FullName", "Title", "NationalPhoneNumber"]);
        services.AddSingleton(parser);
        return services.BuildServiceProvider();
    }

    public async Task<SampleContext> GetSampleContextWithDataAsync(IServiceProvider sp)
    {
        var db = sp.GetRequiredService<SampleContext>();
        var parser = sp.GetRequiredService<SqlQueryParser>();

        var phoneNumberUtil = PhoneNumberUtil.GetInstance();

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
            NationalPhoneNumber = phoneNumberUtil.Parse("(214) 222-2222", "US").NationalNumber.ToString(),
            Salary = 80_000,
            Birthday = new DateOnly(1980, 1, 1),
            DataValues = [new() { Definition = company.DataDefinitions[0], NumberValue = 30 }],
            Companies = [company]
        });
        db.Employees.Add(new Employee
        {
            FullName = "Jane Doe",
            Title = "Software Developer",
            PhoneNumber = "+52 55 1234 5678", // Mexico
            NationalPhoneNumber = phoneNumberUtil.Parse("+52 55 1234 5678", "US").NationalNumber.ToString(),
            Salary = 90_000,
            Birthday = new DateOnly(1972, 11, 6),
            DataValues = [new() { Definition = company.DataDefinitions[0], NumberValue = 23 }],
            Companies = [company]
        });
        await db.SaveChangesAsync();

        await db.Database.ExecuteSqlRawAsync(
            @"IF FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') != 1
              BEGIN
                  RAISERROR('Full-Text Search is not installed', 16, 1);
              END

              IF NOT EXISTS (SELECT * FROM sys.fulltext_catalogs WHERE name = 'ftCatalog')
              BEGIN
                  CREATE FULLTEXT CATALOG ftCatalog AS DEFAULT;
              END

              IF EXISTS (SELECT * FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID('Employees'))
              BEGIN
                  DROP FULLTEXT INDEX ON Employees;
              END

              CREATE FULLTEXT INDEX ON Employees
              (
                  FullName LANGUAGE 1033,
                  NationalPhoneNumber LANGUAGE 1033,
                  Title LANGUAGE 1033
              )
              KEY INDEX PK_Employees
              ON ftCatalog
              WITH (CHANGE_TRACKING = AUTO, STOPLIST = SYSTEM);");

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
                new EntityFieldInfo { Name = "field", FullName = "field", IsNumber = true }
            ]
        };
        string generatedQuery = await GenerateSqlVisitor.RunAsync(result, context);
        Assert.Equal(expected, generatedQuery);
    }
}

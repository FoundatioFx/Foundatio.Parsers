using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class MappingValidationTests
{
    [Theory]
    [InlineData(20, false)]
    [InlineData(20, true)]
    [InlineData(21, false)]
    [InlineData(21, true)]
    [InlineData(29, false)]
    [InlineData(29, true)]
    public async Task ValidateAsync_WithCustomFieldResolverPriority_ValidatesResolvedFields(int priority, bool useFieldMap)
    {
        using var resolver = new ElasticMappingResolver(CreateMapping);
        var parser = new ElasticQueryParser(c =>
        {
            c.UseMappings(resolver).SetValidationOptions(new QueryValidationOptions { AllowUnresolvedFields = false });
            if (useFieldMap)
                c.UseFieldMap(new FieldMap { { "alias", "known" } }, priority);
            else
                c.UseFieldResolver((field, _) => Task.FromResult<string?>(field == "alias" ? "known" : null), priority);
        });

        Assert.True((await parser.ValidateQueryAsync("alias:value")).IsValid);
        Assert.True((await parser.ValidateSortAsync("alias")).IsValid);
        Assert.True((await parser.ValidateAggregationsAsync("terms:alias")).IsValid);
        Assert.NotNull(await parser.BuildQueryAsync("alias:value"));

        var missing = await parser.ValidateQueryAsync("missing:value");
        Assert.False(missing.IsValid);
        Assert.Contains("missing", missing.UnresolvedFields);
    }

    [Theory]
    [InlineData("query")]
    [InlineData("sort")]
    [InlineData("aggregation")]
    public async Task ValidateAsync_WithReusedContextAndMissingAlias_ReportsOriginalField(string queryType)
    {
        using var resolver = new ElasticMappingResolver(CreateMapping);
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver)
            .UseFieldMap(new FieldMap { { "alias", "missing" } })
            .SetValidationOptions(new QueryValidationOptions { AllowUnresolvedFields = false }));
        var context = new ElasticQueryVisitorContext();

        async Task<QueryValidationResult> ValidateAsync(string field) => queryType switch
        {
            "sort" => await parser.ValidateSortAsync(field, context: context),
            "aggregation" => await parser.ValidateAggregationsAsync($"terms:{field}", context: context),
            _ => await parser.ValidateQueryAsync($"{field}:value", context: context)
        };

        Assert.True((await ValidateAsync("known")).IsValid);
        var result = await ValidateAsync("alias");

        Assert.False(result.IsValid);
        Assert.Contains("alias", result.UnresolvedFields);
        Assert.DoesNotContain("missing", result.UnresolvedFields);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ValidateQueryAsync_WithMissingDefaultField_RejectsOnlyUsedFields(bool mixed, bool included)
    {
        int loads = 0;
        using var resolver = new ElasticMappingResolver(() => { loads++; return CreateMapping(); });
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver)
            .SetDefaultFields(mixed ? ["known", "missing"] : ["missing"])
            .UseIncludes(new Dictionary<string, string> { { "fragment", "value" } }));
        var options = new QueryValidationOptions { AllowUnresolvedFields = false };

        var result = await parser.ValidateQueryAsync(included ? "@include:fragment" : "value", options);

        Assert.False(result.IsValid);
        Assert.Single(result.UnresolvedFields, "missing");
        Assert.InRange(loads, 1, mixed ? 2 : 1);
    }

    [Fact]
    public async Task BuildQueryAsync_WithMissingDefaultField_ThrowsValidationException()
    {
        using var resolver = new ElasticMappingResolver(CreateMapping);
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver).SetDefaultFields(["missing"])
            .SetValidationOptions(new QueryValidationOptions { AllowUnresolvedFields = false }));

        var error = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync("value"));

        Assert.Contains("missing", error.Result!.UnresolvedFields);
    }

    [Theory]
    [InlineData("known:value")]
    [InlineData("known:(value)")]
    [InlineData("known:[1 TO 2]")]
    [InlineData("_exists_:known")]
    [InlineData("_missing_:known")]
    public async Task ValidateQueryAsync_WithUnusedMissingDefaultsAndRestrictions_RemainsValid(string query)
    {
        using var resolver = new ElasticMappingResolver(CreateMapping);
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver).UseFieldMap(new FieldMap())
            .SetDefaultFields(["unused-default"]));
        var options = new QueryValidationOptions { AllowUnresolvedFields = false };
        options.RestrictedFields.Add("unused-restriction");

        var result = await parser.ValidateQueryAsync(query, options);

        Assert.True(result.IsValid, result.Message);
        Assert.Empty(result.UnresolvedFields);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ValidateQueryAsync_WithRuntimeDefaultField_PreservesRuntimeResolution(bool dynamic)
    {
        using var resolver = new ElasticMappingResolver(CreateMapping);
        var parser = new ElasticQueryParser(c =>
        {
            c.UseMappings(resolver).SetDefaultFields(["runtime"]);
            if (dynamic)
                c.UseRuntimeFieldResolver(field => Task.FromResult<ElasticRuntimeField?>(field == "runtime" ? new ElasticRuntimeField { Name = "runtime" } : null));
        });
        var context = new ElasticQueryVisitorContext();
        if (!dynamic)
            context.RuntimeFields.Add(new ElasticRuntimeField { Name = "runtime" });

        var result = await parser.ValidateQueryAsync("value", new QueryValidationOptions { AllowUnresolvedFields = false }, context);

        Assert.True(result.IsValid, result.Message);
        Assert.Empty(result.UnresolvedFields);
        Assert.Single(context.RuntimeFields);
    }

    [Fact]
    public async Task ValidateQueryAsync_WithMappedAliasAndRuntimeField_RemainsValid()
    {
        var mapping = CreateMapping();
        mapping.Properties!.Add("server-alias", new FieldAliasProperty { Path = "known" });
        using var resolver = new ElasticMappingResolver(() => mapping);
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver).UseFieldMap(new FieldMap { { "alias", "known" } }));
        var context = new ElasticQueryVisitorContext();
        context.RuntimeFields.Add(new ElasticRuntimeField { Name = "runtime" });

        var result = await parser.ValidateQueryAsync("alias:value OR server-alias:value OR runtime:value",
            new QueryValidationOptions { AllowUnresolvedFields = false }, context);

        Assert.True(result.IsValid, result.Message);
        Assert.Empty(result.UnresolvedFields);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ValidateQueryAsync_WithFailingRuntimeResolver_PreservesValidationError(bool defaultField)
    {
        using var resolver = new ElasticMappingResolver(CreateMapping);
        int calls = 0;
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver).SetDefaultFields(["missing"]).UseRuntimeFieldResolver(_ =>
        {
            calls++;
            throw new InvalidOperationException("runtime lookup failed");
        }));

        var result = await parser.ValidateQueryAsync(defaultField ? "value" : "missing:value", new QueryValidationOptions { AllowUnresolvedFields = false });

        Assert.False(result.IsValid);
        Assert.Contains("runtime lookup failed", result.Message);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ValidateQueryAsync_WithUnresolvedRuntimeField_DoesNotRepeatCallback()
    {
        using var resolver = new ElasticMappingResolver(CreateMapping);
        int calls = 0;
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver).UseRuntimeFieldResolver(_ =>
        {
            calls++;
            return Task.FromResult<ElasticRuntimeField?>(null);
        }));

        var result = await parser.ValidateQueryAsync("missing:value", new QueryValidationOptions { AllowUnresolvedFields = false });

        Assert.False(result.IsValid);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ValidateQueryAsync_WithBlockedDefaultFieldLoader_ReturnsBeforeLoadCompletes()
    {
        var release = new TaskCompletionSource<TypeMapping?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var resolver = ElasticMappingResolver.CreateWithAsyncLoader(_ => release.Task);
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver).SetDefaultFields(["missing"]));

        var validation = parser.ValidateQueryAsync("value", new QueryValidationOptions { AllowUnresolvedFields = false });
        Assert.False(validation.IsCompleted);
        release.SetResult(CreateMapping());
        var result = await validation.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.False(result.IsValid);
        Assert.Single(result.UnresolvedFields, "missing");
    }

    [Fact]
    public async Task ValidateQueryAsync_WithoutMappings_PreservesCustomResolverContract()
    {
        var parser = new ElasticQueryParser(c => c.UseFieldMap(new FieldMap { { "alias", "custom" } }));

        var result = await parser.ValidateQueryAsync("alias:value", new QueryValidationOptions { AllowUnresolvedFields = false });

        Assert.True(result.IsValid, result.Message);
    }

    [Fact]
    public async Task ValidateAggregationsAsync_WithMetadataField_DoesNotRequireMapping()
    {
        using var resolver = new ElasticMappingResolver(CreateMapping);
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver));

        var result = await parser.ValidateAggregationsAsync("@name:custom terms:known", new QueryValidationOptions { AllowUnresolvedFields = false });

        Assert.Empty(result.UnresolvedFields);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ValidateQueryAsync_WithRepeatedRuntimeLookup_SharesOutcomeAndReleasesScope(bool defaults, bool throws)
    {
        int calls = 0;
        using var resolver = new ElasticMappingResolver(CreateMapping);
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver).SetDefaultFields(["missing"])
            .UseIncludes(new Dictionary<string, string> { { "fragment", defaults ? "value other" : "alias:value other:second" } })
            .UseFieldMap(new FieldMap { { "alias", "missing" }, { "other", "missing" } })
            .UseRuntimeFieldResolver(async _ =>
            {
                calls++;
                await Task.Yield();
                if (throws)
                    throw new InvalidOperationException("runtime lookup failed");
                return null;
            }));
        var context = new ElasticQueryVisitorContext();
        var options = new QueryValidationOptions { AllowUnresolvedFields = false };

        Assert.False((await parser.ValidateQueryAsync("@include:fragment", options, context)).IsValid);
        Assert.Equal(1, calls);
        Assert.False((await parser.ValidateQueryAsync("alias:value", options, context)).IsValid);
        Assert.Equal(2, calls);
    }

    private static TypeMapping CreateMapping() => new() { Properties = new Properties { { "known", new KeywordProperty() } } };
}

# Migrating to Elastic.Clients.Elasticsearch (ES 9)

This guide covers breaking changes when upgrading Foundatio.Parsers.ElasticQueries from `NEST` (ES 7) to `Elastic.Clients.Elasticsearch` (ES 9). This is a major version bump to **v8.0**.

## Package Changes

**Before:**
```xml
<PackageReference Include="NEST" Version="7.17.5" />
```

**After:**
```xml
<PackageReference Include="Foundatio.Parsers.ElasticQueries" Version="8.x" />
<!-- Transitively brings in Elastic.Clients.Elasticsearch -->
```

## Namespace Changes

Remove old NEST namespaces and add new ones:

```csharp
// Remove:
using Nest;

// Add (as needed):
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.IndexManagement;
```

## Client Type Changes

| Before (NEST)          | After (ES 9.x)               |
|------------------------|-------------------------------|
| `IElasticClient`       | `ElasticsearchClient`         |
| `ElasticClient`        | `ElasticsearchClient`         |
| `ConnectionSettings`   | `ElasticsearchClientSettings` |

**Before:**
```csharp
var client = new ElasticClient(new ConnectionSettings(new Uri("http://localhost:9200")));
```

**After:**
```csharp
var client = new ElasticsearchClient(new ElasticsearchClientSettings(new Uri("http://localhost:9200")));
```

## ElasticQueryParser API Changes

### BuildQueryAsync

Returns `Query` instead of `QueryContainer`:

**Before:**
```csharp
QueryContainer query = await parser.BuildQueryAsync("status:active");
```

**After:**
```csharp
Query query = await parser.BuildQueryAsync("status:active");
```

### BuildAggregationsAsync

Returns `AggregationMap` instead of `AggregationContainer`. Use `.ToDictionary()` to get the Elasticsearch-native dictionary or the `Aggregations()` extension method on the search descriptor:

**Before:**
```csharp
AggregationContainer aggs = await parser.BuildAggregationsAsync("terms:status");

var response = await client.SearchAsync<MyDoc>(s => s
    .Aggregations(aggs));
```

**After:**
```csharp
using Foundatio.Parsers.ElasticQueries.Extensions;

AggregationMap aggs = await parser.BuildAggregationsAsync("terms:status");

var response = await client.SearchAsync<MyDoc>(s => s
    .Aggregations(aggs));
```

### BuildSortAsync

Returns `ICollection<SortOptions>` instead of `IEnumerable<IFieldSort>`:

**Before:**
```csharp
IEnumerable<IFieldSort> sorts = await parser.BuildSortAsync("created:-1");

var response = await client.SearchAsync<MyDoc>(s => s
    .Sort(sorts));
```

**After:**
```csharp
ICollection<SortOptions> sorts = await parser.BuildSortAsync("created:-1");

var response = await client.SearchAsync<MyDoc>(s => s
    .Sort(sorts));
```

::: tip
The deleted `SearchDescriptorExtensions.Sort()` extension is no longer needed. The ES 9.x `SearchRequestDescriptor<T>.Sort()` accepts `ICollection<SortOptions>` natively.
:::

### UseMappings

The mapping builder parameter changed from `Func` (return-based) to `Action` (mutation-based), and the client type changed:

**Before:**
```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings<MyDoc>(
        m => m.Properties(p => p
            .Text(t => t.Name(n => n.Title))
            .Keyword(k => k.Name(n => n.Status))),
        client));
```

**After:**
```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings<MyDoc>(
        m => m.Properties(p => p
            .Text(n => n.Title)
            .Keyword(n => n.Status)),
        client));
```

Other `UseMappings` overload changes:

| Before                                                         | After                                          |
|----------------------------------------------------------------|------------------------------------------------|
| `UseMappings<T>(Func<..., ...> builder, IElasticClient)`       | `UseMappings<T>(Action<...> builder, ElasticsearchClient)` |
| `UseMappings<T>(Func<..., ...> builder, Inferrer, Func<ITypeMapping>)` | `UseMappings<T>(Action<...> builder, Inferrer, Func<TypeMapping>)` |
| `UseMappings<T>(IElasticClient)`                               | `UseMappings<T>(ElasticsearchClient)`          |
| `UseMappings(IElasticClient, string)`                          | `UseMappings(ElasticsearchClient, string)`     |
| `UseMappings(Func<ITypeMapping>, Inferrer)`                    | `UseMappings(Func<TypeMapping>, Inferrer)`     |

## Query Type Changes

All query types changed from object-initializer style to constructor-based:

| Before (NEST)                                                       | After (ES 9.x)                                        |
|---------------------------------------------------------------------|-------------------------------------------------------|
| `new MatchQuery { Field = "f", Query = "v" }`                      | `new MatchQuery("f", "v")`                            |
| `new MatchPhraseQuery { Field = "f", Query = "v" }`                | `new MatchPhraseQuery("f", "v")`                      |
| `new TermQuery { Field = "f", Value = "v" }`                       | `new TermQuery("f", "v")`                             |
| `new PrefixQuery { Field = "f", Value = "v" }`                     | `new PrefixQuery("f", "v")`                           |
| `new ExistsQuery { Field = "f" }`                                   | `new ExistsQuery("f")`                                |
| `new NestedQuery { Path = "p", Query = q }`                        | `new NestedQuery("p", q)`                             |
| `new GeoDistanceQuery { Field = "f", Location = loc, Distance = d }` | `new GeoDistanceQuery(distance, field, location)`   |
| `new DateRangeQuery { Field = "f", GreaterThanOrEqualTo = v }`     | `new DateRangeQuery("f") { Gte = v }`                 |
| `new DateRangeQuery { Field = "f", LessThan = v }`                 | `new DateRangeQuery("f") { Lt = v }`                  |
| `new TermRangeQuery { Field = "f", GreaterThan = v }`              | `new TermRangeQuery("f") { Gt = v }`                  |
| `new TermRangeQuery { Field = "f", LessThanOrEqualTo = v }`        | `new TermRangeQuery("f") { Lte = v }`                 |
| `new QueryStringQuery { Fields = f, Query = q }`                   | `new QueryStringQuery(q) { Fields = f }`              |
| `new MultiMatchQuery { Fields = f, Query = q }`                    | `new MultiMatchQuery(q) { Fields = f }`               |
| `QueryBase`                                                         | `Query`                                               |
| `QueryContainer`                                                    | `Query`                                               |

### Bool Query Operators

The `&` and `|` operators still work on `Query`, but note that `|` (OR) **no longer** implicitly sets `minimum_should_match: 1`. The parser handles this automatically, but if you build bool queries manually in custom visitors, you must set it explicitly:

```csharp
var boolQuery = new BoolQuery
{
    Should = new List<Query> { queryA, queryB },
    MinimumShouldMatch = 1
};
```

## Aggregation Changes

`AggregationBase` and `AggregationContainer` are replaced by `AggregationMap`, a custom intermediate type that bridges the gap between the parser's tree-building phase and the ES 9.x client's discriminated-union `Aggregation` type.

**Before:**
```csharp
AggregationContainer aggs = await parser.BuildAggregationsAsync("terms:status");
```

**After:**
```csharp
AggregationMap aggs = await parser.BuildAggregationsAsync("terms:status");

// Convert to ES client dictionary when needed:
IDictionary<string, Aggregation> dict = aggs.ToDictionary();
```

## Sort Changes

| Before (NEST)            | After (ES 9.x)  |
|--------------------------|------------------|
| `IFieldSort`             | `SortOptions`    |
| `ISort`                  | `SortOptions`    |
| `SortOrder.Ascending`    | `SortOrder.Asc`  |
| `SortOrder.Descending`   | `SortOrder.Desc` |

The `SearchDescriptorExtensions.Sort()` extension method has been removed. Use the native `SearchRequestDescriptor<T>.Sort()` method directly.

## Property Mapping Changes

The ES 9.x client uses a simpler expression-based syntax:

| Before (NEST)                                                | After (ES 9.x)                                     |
|--------------------------------------------------------------|-----------------------------------------------------|
| `.Keyword(f => f.Name(e => e.Id))`                           | `.Keyword(e => e.Id)`                               |
| `.Text(f => f.Name(e => e.Name))`                            | `.Text(e => e.Name)`                                |
| `.Number(f => f.Name(e => e.Age).Type(NumberType.Integer))`  | `.IntegerNumber(e => e.Age)`                        |
| `.Date(f => f.Name(e => e.Created))`                         | `.Date(e => e.Created)`                             |
| `.Boolean(f => f.Name(e => e.Active))`                       | `.Boolean(e => e.Active)`                           |
| `.Object<T>(f => f.Name(e => e.Address).Properties(...))`    | `.Object(e => e.Address, o => o.Properties(...))` |
| `.Nested<T>(f => f.Name(e => e.Items).Properties(...))`      | `.Nested(e => e.Items, n => n.Properties(...))`   |
| `.Dynamic(false)`                                            | `.Dynamic(DynamicMapping.False)`                    |
| `m.AutoMap()`                                                | *(removed — define all properties explicitly)*      |

## Custom Visitor Migration

If you have custom visitors that set queries, aggregations, or sorts on nodes, update the types:

**Before:**
```csharp
public class MyVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        QueryBase query = new TermQuery { Field = "status", Value = "active" };
        node.SetQuery(query);
    }
}
```

**After:**
```csharp
public class MyVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        Query query = new TermQuery("status", "active");
        node.SetQuery(query);
    }
}
```

Key type changes in the visitor API:

| Method / Property | Before              | After           |
|-------------------|---------------------|-----------------|
| `SetQuery()`      | `QueryBase`         | `Query`         |
| `GetQueryAsync()`  | `Task<QueryBase>`  | `Task<Query>`   |
| `SetAggregation()` | `AggregationBase`  | `AggregationMap` |
| `GetAggregationAsync()` | `Task<AggregationBase>` | `Task<AggregationMap>` |
| `SetSort()`       | `IFieldSort`        | `SortOptions`   |
| `GetSort()`       | `IFieldSort`        | `SortOptions`   |

## IElasticQueryVisitorContext Changes

A new property was added to the `IElasticQueryVisitorContext` interface:

```csharp
Func<string, Task<string>> GeoLocationResolver { get; set; }
```

This enables per-request geo location resolution (e.g., converting zip codes to lat/lon). If you implement `IElasticQueryVisitorContext` directly (rather than inheriting from `ElasticQueryVisitorContext`), you must add this property.

## Response Validation

| Before (NEST)              | After (ES 9.x)                |
|----------------------------|-------------------------------|
| `response.IsValid`         | `response.IsValidResponse`    |

## Behavioral Changes

These are not breaking API changes but produce different (improved) Elasticsearch JSON:

### Filter-mode queries are flatter

Non-scoring queries now produce flat `bool { filter: [a, b, c] }` instead of nested `bool { filter: [bool { must: [a, b, c] }] }`. Functionally equivalent but cleaner.

### Explicit minimum_should_match on OR queries

NEST's `|` operator implicitly set `minimum_should_match: 1`. The ES 9.x client does not, so the parser now explicitly sets it on root-level OR queries and parenthesized OR groups inside AND context.

### Nested query negation

Negated nested terms (e.g., `NOT nested.field:value`) now negate the entire `NestedQuery` rather than the inner query. This is a correctness fix — the new behavior correctly excludes documents that have any matching nested object.

### Integer fields map to long

ES 9.x auto-mapping maps C# `int` properties to `long` instead of `integer`. This means `@field_type` metadata on aggregations will report `"long"` instead of `"integer"` for `int`-typed fields.

## Migration Checklist

- [ ] Replace `using Nest;` with `using Elastic.Clients.Elasticsearch;` (and sub-namespaces)
- [ ] Replace `IElasticClient` / `ElasticClient` with `ElasticsearchClient`
- [ ] Replace `ConnectionSettings` with `ElasticsearchClientSettings`
- [ ] Update `BuildQueryAsync` return type from `QueryContainer` to `Query`
- [ ] Update `BuildAggregationsAsync` return type from `AggregationContainer` to `AggregationMap`
- [ ] Update `BuildSortAsync` return type from `IEnumerable<IFieldSort>` to `ICollection<SortOptions>`
- [ ] Update `UseMappings` calls (change `Func<..., ...>` to `Action<...>`, change client type)
- [ ] Update property mapping syntax (remove `.Name(e => e.Prop)` wrapper)
- [ ] Replace `AutoMap()` calls with explicit property mappings
- [ ] Replace `.Dynamic(false)` with `.Dynamic(DynamicMapping.False)`
- [ ] Replace `response.IsValid` with `response.IsValidResponse`
- [ ] Update custom visitors to use `Query` instead of `QueryBase`/`QueryContainer`
- [ ] Update custom visitors to use `AggregationMap` instead of `AggregationBase`
- [ ] Update custom visitors to use `SortOptions` instead of `IFieldSort`
- [ ] Add `GeoLocationResolver` property if directly implementing `IElasticQueryVisitorContext`
- [ ] Remove `SearchDescriptorExtensions.Sort()` usage (use native `.Sort()`)
- [ ] Remove NEST package reference
- [ ] Update Elasticsearch/Kibana Docker images to 9.x

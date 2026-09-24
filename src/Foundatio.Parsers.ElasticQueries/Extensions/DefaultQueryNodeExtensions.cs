using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class DefaultQueryNodeExtensions
{
    public static async Task<Query?> GetDefaultQueryAsync(this IQueryNode node, IQueryVisitorContext context)
    {
        if (node is TermNode termNode)
            return await termNode.GetDefaultQueryAsync(context).AnyContext();

        if (node is TermRangeNode termRangeNode)
            return await termRangeNode.GetDefaultQueryAsync(context).AnyContext();

        if (node is ExistsNode existsNode)
            return existsNode.GetDefaultQuery(context);

        if (node is MissingNode missingNode)
            return missingNode.GetDefaultQuery(context);

        return null;
    }

    public static async Task<Query?> GetDefaultQueryAsync(this TermNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        // Keep absent terms nullable at this public boundary. Every successfully read
        // QueryTerm produces a query, so mapping-specific helpers do not need null fallbacks.
        if (node.Term is null)
            return null;

        if (!QueryTerm.TryCreate(node, elasticContext, out var term))
            return new MatchNoneQuery();

        using var mappingScope = context.BeginMappingScope();
        string? field = node.UnescapedField;
        string[]? defaultFields = node.GetDefaultFields(elasticContext.DefaultFields);

        if (!String.IsNullOrEmpty(field))
        {
            await context.GetMappingResultAsync(field).AnyContext();
            return GetSingleFieldQuery(term, field, elasticContext);
        }

        if (defaultFields is not null)
        {
            foreach (string defaultField in defaultFields)
                await context.GetMappingResultAsync(defaultField).AnyContext();
        }

        if (defaultFields is { Length: 1 })
        {
            var singleQuery = GetSingleFieldQuery(term, defaultFields[0], elasticContext);
            string? nestedPath = NestedPathResolver.GetDeepestNestedPath(defaultFields[0], elasticContext);
            if (nestedPath is not null)
            {
                Query innerQuery = singleQuery;
                var filterResolver = GetNestedFilterResolver(elasticContext);
                if (filterResolver is not null)
                {
                    var filter = await filterResolver(nestedPath, defaultFields[0], defaultFields[0], context).AnyContext();
                    if (filter is not null)
                        innerQuery = new BoolQuery { Must = [innerQuery], Filter = [filter] };
                }

                return new NestedQuery(nestedPath, innerQuery);
            }

            return singleQuery;
        }

        if (defaultFields is { Length: > 1 })
        {
            // Keep root fields together and give each nested path its own query boundary.
            var fieldsByNestedPath = GroupFieldsByNestedPath(defaultFields, elasticContext);
            if (fieldsByNestedPath.Count == 1 && fieldsByNestedPath.ContainsKey(String.Empty))
                return GetMultiFieldQuery(term, defaultFields, elasticContext);

            return await GetSplitNestedQueryAsync(term, fieldsByNestedPath, elasticContext).AnyContext();
        }

        return GetMultiFieldQuery(term, defaultFields, elasticContext);
    }

    /// <summary>
    /// Synchronous compatibility shim. Prefer <see cref="GetDefaultQueryAsync(TermNode, IQueryVisitorContext)"/> for async nested filter resolution.
    /// </summary>
    [Obsolete("Use GetDefaultQueryAsync to support async nested filter resolution.")]
    public static Query? GetDefaultQuery(this TermNode node, IQueryVisitorContext context)
    {
        if (SynchronizationContext.Current is not null || TaskScheduler.Current != TaskScheduler.Default)
            return Task.Run(() => GetDefaultQueryAsync(node, context)).GetAwaiter().GetResult();

        return GetDefaultQueryAsync(node, context).AnyContext().GetAwaiter().GetResult();
    }

    private static Query GetSingleFieldQuery(QueryTerm term, string field, IElasticQueryVisitorContext context)
    {
        var fieldType = ElasticMappingResolver.GetFieldType(context.GetMappingResult(field)?.Property);
        if ((term.Regex is not null || term.Fuzziness is not null)
            && fieldType is FieldType.Byte or FieldType.Short or FieldType.Integer or FieldType.Long
                or FieldType.Float or FieldType.HalfFloat or FieldType.Double or FieldType.ScaledFloat
                or FieldType.TokenCount or FieldType.Date or FieldType.DateNanos or FieldType.Boolean)
        {
            context.AddValidationError($"Regex and fuzzy queries are not supported on field '{field}' of type '{fieldType}'.");
            return new MatchNoneQuery();
        }

        if (term.Regex is not null)
            return new RegexpQuery(field, term.Regex) { Boost = term.Boost };

        bool analyzed = IsPropertyAnalyzed(field, context);
        if (term.Wildcard is not null)
        {
            if (term.Wildcard == "*")
                return new ExistsQuery(field) { Boost = term.Boost };

            // Match queries analyze '*' as text; query_string retains wildcard operators
            // while applying the field analyzer to the safely escaped term expression.
            if (analyzed)
                return term.ToQueryString([field], context);

            if (term.IsPrefix)
                return new PrefixQuery(field, term.Value[..^1]) { Boost = term.Boost };

            return new WildcardQuery(field) { Value = term.Wildcard, Boost = term.Boost };
        }

        if (term.Quoted && (analyzed || term.Slop is not null))
            return new MatchPhraseQuery(field, term.Value) { Slop = term.Slop, Boost = term.Boost };

        if (analyzed)
            return new MatchQuery(field, term.Value) { Fuzziness = term.Fuzziness, Boost = term.Boost };

        if (term.Fuzziness is not null)
            return new FuzzyQuery(field, term.Value) { Fuzziness = term.Fuzziness, Boost = term.Boost };

        // Non-analyzed numeric and boolean fields need typed values, not JSON strings.
        FieldValue termValue = GetTypedFieldValue(term.Value, field, context);
        return new TermQuery(field, termValue) { Boost = term.Boost };
    }

    private static FieldValue GetTypedFieldValue(string value, string field, IElasticQueryVisitorContext context)
    {
        var fieldType = ElasticMappingResolver.GetFieldType(context.GetMappingResult(field)?.Property);

        return fieldType switch
        {
            FieldType.Integer or FieldType.Short or FieldType.Byte when Int32.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int intValue) => intValue,
            FieldType.Long when Int64.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longValue) => longValue,
            FieldType.Float or FieldType.HalfFloat when Single.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out float floatValue) => floatValue,
            FieldType.Double or FieldType.ScaledFloat when Double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double doubleValue) => doubleValue,
            FieldType.Boolean when Boolean.TryParse(value, out bool boolValue) => boolValue,
            _ => value
        };
    }

    private static Query GetMultiFieldQuery(QueryTerm term, string[]? fields, IElasticQueryVisitorContext context)
    {
        if (fields is null or { Length: 0 })
            return GetAnalyzedFieldsQuery(term, null, context);

        var analyzedFields = new List<string>();
        var nonAnalyzedFields = new List<string>();

        foreach (string field in fields)
        {
            if (IsPropertyAnalyzed(field, context))
                analyzedFields.Add(field);
            else
                nonAnalyzedFields.Add(field);
        }

        if (nonAnalyzedFields.Count == 0)
            return GetAnalyzedFieldsQuery(term, analyzedFields.ToArray(), context);

        if (analyzedFields.Count == 0)
            return GetNonAnalyzedFieldsQuery(term, nonAnalyzedFields, context);

        // multi_match cannot preserve each keyword/numeric field's exact-value semantics.
        // Combine analyzed fields with separately typed non-analyzed queries instead.
        var queries = new List<Query> { GetAnalyzedFieldsQuery(term, analyzedFields.ToArray(), context) };
        foreach (string field in nonAnalyzedFields)
            queries.Add(GetSingleFieldQuery(term, field, context));

        return new BoolQuery { Should = queries };
    }

    private static Query GetAnalyzedFieldsQuery(QueryTerm term, string[]? fields, IElasticQueryVisitorContext context)
    {
        if (term.Wildcard is not null)
            return term.ToQueryString(fields, context);

        if (term.Regex is not null)
        {
            if (fields is null or { Length: 0 })
                return term.ToQueryString(fields, context);

            return new BoolQuery { Should = fields.Select(field => (Query)new RegexpQuery(field, term.Regex) { Boost = term.Boost }).ToArray() };
        }

        if (fields is { Length: 1 })
            return GetSingleFieldQuery(term, fields[0], context);

        var query = new MultiMatchQuery(term.Value)
        {
            Type = term.Quoted ? TextQueryType.Phrase : null,
            Slop = term.Slop,
            Fuzziness = term.Fuzziness,
            Boost = term.Boost
        };
        if (fields is { Length: > 0 })
            query.Fields = fields;
        return query;
    }

    private static Query GetNonAnalyzedFieldsQuery(QueryTerm term, List<string> fields, IElasticQueryVisitorContext context)
    {
        if (fields.Count == 1)
            return GetSingleFieldQuery(term, fields[0], context);

        return new BoolQuery { Should = fields.Select(field => GetSingleFieldQuery(term, field, context)).ToArray() };
    }

    private static Dictionary<string, List<string>> GroupFieldsByNestedPath(string[] fields, IElasticQueryVisitorContext context)
    {
        var result = new Dictionary<string, List<string>>();

        foreach (string field in fields)
        {
            // The empty key groups root fields; nested fields use their deepest mapped path.
            string nestedPath = NestedPathResolver.GetDeepestNestedPath(field, context) ?? String.Empty;
            if (!result.ContainsKey(nestedPath))
                result[nestedPath] = new List<string>();
            result[nestedPath].Add(field);
        }

        return result;
    }

    private static bool IsPropertyAnalyzed(string field, IElasticQueryVisitorContext context)
    {
        var mapping = context.GetMappingResult(field);
        return mapping?.Found is true && context.MappingResolver.IsPropertyAnalyzed(mapping.Property!);
    }

    private static async Task<Query> GetSplitNestedQueryAsync(QueryTerm term, Dictionary<string, List<string>> fieldsByNestedPath, IElasticQueryVisitorContext context)
    {
        var queryList = new List<Query>();

        foreach (var (nestedPath, fields) in fieldsByNestedPath)
        {
            if (!String.IsNullOrEmpty(nestedPath))
            {
                var filterResolver = GetNestedFilterResolver(context);
                if (filterResolver is not null)
                {
                    var branches = new List<Query>();
                    foreach (string field in fields)
                    {
                        Query branch = GetSingleFieldQuery(term, field, context);
                        var filter = await filterResolver(nestedPath, field, field, context).AnyContext();
                        if (filter is not null)
                            branch = new BoolQuery { Must = [branch], Filter = [filter] };
                        branches.Add(branch);
                    }

                    // Every path group contains at least one field, each producing a query.
                    Query innerQuery = branches.Count == 1 ? branches[0] : new BoolQuery { Should = branches };
                    queryList.Add(new NestedQuery(nestedPath, innerQuery));
                }
                else
                {
                    Query query = fields.Count == 1
                        ? GetSingleFieldQuery(term, fields[0], context)
                        : GetMultiFieldQuery(term, fields.ToArray(), context);
                    queryList.Add(new NestedQuery(nestedPath, query));
                }
            }
            else
            {
                Query query = fields.Count == 1
                    ? GetSingleFieldQuery(term, fields[0], context)
                    : GetMultiFieldQuery(term, fields.ToArray(), context);

                if (query is { Bool: { Should: not null } boolQuery })
                {
                    foreach (var shouldClause in boolQuery.Should)
                        queryList.Add(shouldClause);
                }
                else
                {
                    queryList.Add(query);
                }
            }
        }

        return new BoolQuery { Should = queryList };
    }

    private static NestedFilterResolver? GetNestedFilterResolver(IElasticQueryVisitorContext context)
    {
        if (context.Data.TryGetValue("@NestedFilterResolver", out object? value))
            return value as NestedFilterResolver;

        return null;
    }

    public static async Task<Query?> GetDefaultQueryAsync(this TermRangeNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        string? field = node.UnescapedField;
        if (String.IsNullOrEmpty(field))
        {
            if (node.Parent is GroupNode parentGroup && !String.IsNullOrEmpty(parentGroup.UnescapedField))
                field = parentGroup.UnescapedField;
            else
                return null;
        }

        var mapping = await context.GetMappingResultAsync(field).AnyContext();
        if (mapping?.Property is DateProperty or DateNanosProperty)
        {
            var range = new DateRangeQuery(field) { TimeZone = node.Boost ?? node.GetTimeZone(await elasticContext.GetTimeZoneAsync().AnyContext()) };
            if (!String.IsNullOrWhiteSpace(node.UnescapedMin) && node.UnescapedMin != "*")
            {
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.Gt = node.UnescapedMin;
                else
                    range.Gte = node.UnescapedMin;
            }

            if (!String.IsNullOrWhiteSpace(node.UnescapedMax) && node.UnescapedMax != "*")
            {
                if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                    range.Lt = node.UnescapedMax;
                else
                    range.Lte = node.UnescapedMax;
            }

            return range;
        }
        else
        {
            if (!QueryTerm.TryReadBoost(node.UnescapedBoost, context, out var boost))
                return new MatchNoneQuery();

            var range = new TermRangeQuery(field) { Boost = boost };
            if (!String.IsNullOrWhiteSpace(node.UnescapedMin) && node.UnescapedMin != "*")
            {
                if (node.MinInclusive.HasValue && !node.MinInclusive.Value)
                    range.Gt = node.UnescapedMin;
                else
                    range.Gte = node.UnescapedMin;
            }

            if (!String.IsNullOrWhiteSpace(node.UnescapedMax) && node.UnescapedMax != "*")
            {
                if (node.MaxInclusive.HasValue && !node.MaxInclusive.Value)
                    range.Lt = node.UnescapedMax;
                else
                    range.Lte = node.UnescapedMax;
            }

            return range;
        }
    }

    public static Query? GetDefaultQuery(this ExistsNode node, IQueryVisitorContext context)
    {
        if (node.UnescapedField is not { } field)
            return null;

        return new ExistsQuery(field);
    }

    public static Query? GetDefaultQuery(this MissingNode node, IQueryVisitorContext context)
    {
        if (node.UnescapedField is not { } field)
            return null;

        return new BoolQuery { MustNot = [new ExistsQuery(field)] };
    }
}

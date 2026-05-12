using System;
using System.Collections.Generic;
using System.Linq;
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
            return await termNode.GetDefaultQueryAsync(context).ConfigureAwait(false);

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

        string? field = node.UnescapedField;
        string[]? defaultFields = node.GetDefaultFields(elasticContext.DefaultFields);

        // If a specific field is set, use single-field query
        if (!String.IsNullOrEmpty(field))
            return GetSingleFieldQuery(node, field, elasticContext);

        // If only one default field, use single-field query (wrapped in nested if applicable)
        if (defaultFields is { Length: 1 })
        {
            var singleQuery = GetSingleFieldQuery(node, defaultFields[0], elasticContext);
            if (singleQuery is null)
                return null;

            string? nestedPath = GetNestedPath(defaultFields[0], elasticContext);
            if (nestedPath is not null)
            {
                Query innerQuery = singleQuery;
                var filterResolver = GetNestedFilterResolver(elasticContext);
                if (filterResolver is not null)
                {
                    var filter = await filterResolver(nestedPath, defaultFields[0], defaultFields[0], context).ConfigureAwait(false);
                    if (filter is not null)
                        innerQuery = new BoolQuery { Must = [innerQuery], Filter = [filter] };
                }

                return new NestedQuery(nestedPath, innerQuery);
            }

            return singleQuery;
        }

        if (defaultFields is { Length: > 1 })
        {
            // Group fields by nested path (empty string for non-nested)
            var fieldsByNestedPath = GroupFieldsByNestedPath(defaultFields, elasticContext);

            // If all fields are non-nested (single group with empty key), use multi_match
            if (fieldsByNestedPath.Count == 1 && fieldsByNestedPath.ContainsKey(String.Empty))
            {
                return GetMultiFieldQuery(node, defaultFields, elasticContext);
            }

            // Otherwise, split into separate queries for each group
            return await GetSplitNestedQueryAsync(node, fieldsByNestedPath, elasticContext).ConfigureAwait(false);
        }

        // Fallback for no fields
        return GetMultiFieldQuery(node, defaultFields, elasticContext);
    }

    /// <summary>
    /// Synchronous compatibility shim. Prefer <see cref="GetDefaultQueryAsync(TermNode, IQueryVisitorContext)"/> for async nested filter resolution.
    /// </summary>
    [Obsolete("Use GetDefaultQueryAsync to support async nested filter resolution.")]
    public static Query? GetDefaultQuery(this TermNode node, IQueryVisitorContext context)
    {
        return GetDefaultQueryAsync(node, context).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    private static Query? GetSingleFieldQuery(TermNode node, string field, IElasticQueryVisitorContext context)
    {
        if (context.MappingResolver.IsPropertyAnalyzed(field))
        {
            if (node.UnescapedTerm is not { } term)
                return null;

            // MatchQuery treats '*' as literal; use QueryStringQuery for wildcard support on analyzed fields
            if (!node.IsQuotedTerm && term.EndsWith("*"))
            {
                return new QueryStringQuery(term)
                {
                    Fields = new[] { field },
                    AllowLeadingWildcard = false,
                    AnalyzeWildcard = true
                };
            }

            if (node.IsQuotedTerm)
                return new MatchPhraseQuery(field, term);

            return new MatchQuery(field, term);
        }

        if (!node.IsQuotedTerm && node.UnescapedTerm?.EndsWith("*") is true)
            return new PrefixQuery(field, node.UnescapedTerm.TrimEnd('*'));

        if (node.UnescapedTerm is null)
            return null;

        // For non-analyzed fields, convert value to appropriate type
        FieldValue termValue = GetTypedFieldValue(node.UnescapedTerm, field, context);
        return new TermQuery(field, termValue);
    }

    private static FieldValue GetTypedFieldValue(string value, string field, IElasticQueryVisitorContext context)
    {
        var fieldType = context.MappingResolver.GetFieldType(field);

        return fieldType switch
        {
            FieldType.Integer or FieldType.Short or FieldType.Byte when Int32.TryParse(value, out int intValue) => intValue,
            FieldType.Long when Int64.TryParse(value, out long longValue) => longValue,
            FieldType.Float or FieldType.HalfFloat when Single.TryParse(value, out float floatValue) => floatValue,
            FieldType.Double or FieldType.ScaledFloat when Double.TryParse(value, out double doubleValue) => doubleValue,
            FieldType.Boolean when Boolean.TryParse(value, out bool boolValue) => boolValue,
            _ => value
        };
    }

    private static Query? GetMultiFieldQuery(TermNode node, string[]? fields, IElasticQueryVisitorContext context)
    {
        // Handle null or empty fields - use default multi_match behavior
        if (fields is null or { Length: 0 })
        {
            if (node.UnescapedTerm is null)
                return null;

            var defaultQuery = new MultiMatchQuery(node.UnescapedTerm);
            if (node.IsQuotedTerm)
                defaultQuery.Type = TextQueryType.Phrase;
            return defaultQuery;
        }

        // Split fields by analyzed vs non-analyzed
        var analyzedFields = new List<string>();
        var nonAnalyzedFields = new List<string>();

        foreach (string field in fields)
        {
            if (context.MappingResolver.IsPropertyAnalyzed(field))
                analyzedFields.Add(field);
            else
                nonAnalyzedFields.Add(field);
        }

        if (nonAnalyzedFields.Count == 0)
            return GetAnalyzedFieldsQuery(node, analyzedFields.ToArray());

        if (analyzedFields.Count == 0)
            return GetNonAnalyzedFieldsQuery(node, nonAnalyzedFields, context);

        // multi_match doesn't work well across analyzed + non-analyzed field types,
        // so split into separate queries and combine with bool should.
        var queries = new List<Query>();
        var analyzedQuery = GetAnalyzedFieldsQuery(node, analyzedFields.ToArray());
        if (analyzedQuery is not null)
            queries.Add(analyzedQuery);

        foreach (string field in nonAnalyzedFields)
        {
            var query = GetSingleFieldQuery(node, field, context);
            if (query is not null)
                queries.Add(query);
        }

        return new BoolQuery { Should = queries };
    }

    private static Query? GetAnalyzedFieldsQuery(TermNode node, string[] fields)
    {
        if (node.UnescapedTerm is not { } term)
            return null;

        if (!node.IsQuotedTerm && term.EndsWith("*"))
        {
            return new QueryStringQuery(term)
            {
                Fields = fields,
                AllowLeadingWildcard = false,
                AnalyzeWildcard = true
            };
        }

        if (fields.Length == 1)
        {
            if (node.IsQuotedTerm)
                return new MatchPhraseQuery(fields[0], term);

            return new MatchQuery(fields[0], term);
        }

        var query = new MultiMatchQuery(term);
        query.Fields = fields;
        if (node.IsQuotedTerm)
            query.Type = TextQueryType.Phrase;

        return query;
    }

    private static Query? GetNonAnalyzedFieldsQuery(TermNode node, List<string> fields, IElasticQueryVisitorContext context)
    {
        if (fields.Count == 1)
            return GetSingleFieldQuery(node, fields[0], context);

        var queries = fields.Select(f => GetSingleFieldQuery(node, f, context)).Where(q => q is not null).Cast<Query>().ToList();
        return new BoolQuery { Should = queries };
    }

    private static Dictionary<string, List<string>> GroupFieldsByNestedPath(string[] fields, IElasticQueryVisitorContext context)
    {
        var result = new Dictionary<string, List<string>>();

        foreach (string field in fields)
        {
            // Use empty string for non-nested fields, actual path for nested
            string nestedPath = GetNestedPath(field, context) ?? String.Empty;

            if (!result.ContainsKey(nestedPath))
                result[nestedPath] = new List<string>();

            result[nestedPath].Add(field);
        }

        return result;
    }

    private static string? GetNestedPath(string fullName, IElasticQueryVisitorContext context)
    {
        return NestedPathResolver.GetDeepestNestedPath(fullName, context.MappingResolver);
    }

    private static async Task<Query> GetSplitNestedQueryAsync(TermNode node, Dictionary<string, List<string>> fieldsByNestedPath, IElasticQueryVisitorContext context)
    {
        var queryList = new List<Query>();

        foreach (var (nestedPath, fields) in fieldsByNestedPath)
        {
            if (!String.IsNullOrEmpty(nestedPath))
            {
                var filterResolver = GetNestedFilterResolver(context);
                if (filterResolver is not null)
                {
                    // Build per-field branches to preserve distinct filters
                    var branches = new List<Query>();
                    foreach (string field in fields)
                    {
                        var q = GetSingleFieldQuery(node, field, context);
                        if (q is null)
                            continue;

                        Query branch = q;
                        var filter = await filterResolver(nestedPath, field, field, context).ConfigureAwait(false);
                        if (filter is not null)
                            branch = new BoolQuery { Must = [branch], Filter = [filter] };
                        branches.Add(branch);
                    }

                    if (branches.Count is 0)
                        continue;

                    Query innerQuery = branches.Count == 1
                        ? branches[0]
                        : new BoolQuery { Should = branches };

                    queryList.Add(new NestedQuery(nestedPath, innerQuery));
                }
                else
                {
                    Query? query = fields.Count == 1
                        ? GetSingleFieldQuery(node, fields[0], context)
                        : GetMultiFieldQuery(node, fields.ToArray(), context);

                    if (query is not null)
                        queryList.Add(new NestedQuery(nestedPath, query));
                }
            }
            else
            {
                Query? query = fields.Count == 1
                    ? GetSingleFieldQuery(node, fields[0], context)
                    : GetMultiFieldQuery(node, fields.ToArray(), context);

                if (query is null)
                    continue;

                // Flatten inner should clauses to avoid unnecessary bool nesting
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
        if (context is IQueryVisitorContext visitorContext &&
            visitorContext.Data.TryGetValue("@NestedFilterResolver", out object? value))
            return value as NestedFilterResolver;

        return null;
    }

    public static async Task<Query?> GetDefaultQueryAsync(this TermRangeNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        string? field = node.UnescapedField;
        if (String.IsNullOrEmpty(field))
            return null;

        if (elasticContext.MappingResolver.IsDatePropertyType(field))
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
            var range = new TermRangeQuery(field);
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

        return new BoolQuery
        {
            MustNot =
            [
                new ExistsQuery(field)
            ]
        };
    }
}

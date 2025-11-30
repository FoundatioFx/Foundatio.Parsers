using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Elastic.Transport.Products.Elasticsearch;
using Microsoft.Extensions.Logging;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class ElasticExtensions
{
    public static bool TryGet<T>(this Query query, out T result)
    {
        // TODO: until: https://github.com/elastic/elasticsearch-net/issues/8496
        result = default;
        return false;
    }

    public static string TryGetName(this IProperty property)
    {
        // TODO: until: https://github.com/elastic/elasticsearch-net/issues/8336
        return null;
    }

    public static bool IsBucketAggregation(this object aggregation)
    {
        // NOTE FilterAggregate was called FilterAggregation in the past.
        return aggregation is AdjacencyMatrixAggregation or AutoDateHistogramAggregation or ChildrenAggregation
            or CompositeAggregation or DateHistogramAggregation or DateRangeAggregation or DiversifiedSamplerAggregation
            or FilterAggregate or FiltersAggregation or GeoDistanceAggregation or GeohashGridAggregation
            or GeotileGridAggregation or GlobalAggregation or HistogramAggregation or IpRangeAggregation
            or MissingAggregation or MultiTermsAggregation or NestedAggregation or ParentAggregation or RangeAggregation
            or RareTermsAggregation or ReverseNestedAggregation or SamplerAggregation or SignificantTermsAggregation
            or SignificantTextAggregation or TermsAggregation or VariableWidthHistogramAggregation;
    }

    public static Properties GetFields(this IProperty property)
    {
        return property switch
        {
            AggregateMetricDoubleProperty p => p.Fields,
            BinaryProperty p => p.Fields,
            BooleanProperty p => p.Fields,
            ByteNumberProperty p => p.Fields,
            CompletionProperty p => p.Fields,
            ConstantKeywordProperty p => p.Fields,
            DateNanosProperty p => p.Fields,
            DateProperty p => p.Fields,
            DateRangeProperty p => p.Fields,
            DenseVectorProperty p => p.Fields,
            DoubleNumberProperty p => p.Fields,
            DoubleRangeProperty p => p.Fields,
            DynamicProperty p => p.Fields,
            FieldAliasProperty p => p.Fields,
            FlattenedProperty p => p.Fields,
            FloatNumberProperty p => p.Fields,
            FloatRangeProperty p => p.Fields,
            GeoPointProperty p => p.Fields,
            GeoShapeProperty p => p.Fields,
            HalfFloatNumberProperty p => p.Fields,
            HistogramProperty p => p.Fields,
            IcuCollationProperty p => p.Fields,
            IntegerNumberProperty p => p.Fields,
            IntegerRangeProperty p => p.Fields,
            IpProperty p => p.Fields,
            IpRangeProperty p => p.Fields,
            JoinProperty p => p.Fields,
            KeywordProperty p => p.Fields,
            LongNumberProperty p => p.Fields,
            LongRangeProperty p => p.Fields,
            MatchOnlyTextProperty p => p.Fields,
            Murmur3HashProperty p => p.Fields,
            NestedProperty p => p.Fields,
            ObjectProperty p => p.Fields,
            PercolatorProperty p => p.Fields,
            PointProperty p => p.Fields,
            RankFeatureProperty p => p.Fields,
            RankFeaturesProperty p => p.Fields,
            ScaledFloatNumberProperty p => p.Fields,
            SearchAsYouTypeProperty p => p.Fields,
            ShapeProperty p => p.Fields,
            ShortNumberProperty p => p.Fields,
            SparseVectorProperty p => p.Fields,
            TextProperty p => p.Fields,
            TokenCountProperty p => p.Fields,
            UnsignedLongNumberProperty p => p.Fields,
            VersionProperty p => p.Fields,
            WildcardProperty p => p.Fields,
            _ => null
        };
    }

    public static TermsInclude AddValue(this TermsInclude include, string value)
    {
        if (include?.Values == null)
            return new TermsInclude([value]);

        var values = include.Values.ToList();
        values.Add(value);

        return new TermsInclude(values);
    }

    public static TermsExclude AddValue(this TermsExclude exclude, string value)
    {
        if (exclude?.Values == null)
            return new TermsExclude([value]);

        var values = exclude.Values.ToList();
        values.Add(value);

        return new TermsExclude(values);
    }

    // TODO: Handle IFailureReason/BulkIndexByScrollFailure and other bulk response types.
    public static string GetErrorMessage(this ElasticsearchResponse elasticResponse, string message = null, bool normalize = false, bool includeResponse = false, bool includeDebugInformation = false)
    {
        if (elasticResponse == null)
            return String.Empty;

        var sb = new StringBuilder();

        if (!String.IsNullOrEmpty(message))
            sb.AppendLine(message);

        if (includeDebugInformation && elasticResponse?.DebugInformation != null)
            sb.AppendLine(elasticResponse.DebugInformation);

        if (elasticResponse.TryGetOriginalException(out var exception) && exception is not null)
            sb.AppendLine($"Original: [{exception.GetType().Name}] {exception.Message}");

        if (elasticResponse.ElasticsearchServerError?.Error != null)
            sb.AppendLine($"Server Error (Index={elasticResponse.ElasticsearchServerError.Error?.Index}): {elasticResponse.ElasticsearchServerError.Error.Reason}");

        if (elasticResponse is BulkResponse bulkResponse)
            sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

        var apiCall = elasticResponse.ApiCallDetails;
        if (apiCall is not null)
            sb.AppendLine($"[{apiCall.HttpStatusCode}] {apiCall.HttpMethod} {apiCall.Uri?.PathAndQuery}");

        if (apiCall?.RequestBodyInBytes is not null)
        {
            string body = Encoding.UTF8.GetString(apiCall.RequestBodyInBytes);
            if (normalize)
                body = JsonUtility.Normalize(body);
            sb.AppendLine(body);
        }

        if (includeResponse && apiCall.ResponseBodyInBytes != null && apiCall.ResponseBodyInBytes.Length > 0 && apiCall.ResponseBodyInBytes.Length < 20000)
        {
            string body = Encoding.UTF8.GetString(apiCall?.ResponseBodyInBytes);
            if (normalize)
                body = JsonUtility.Normalize(body);

            if (!String.IsNullOrWhiteSpace(body))
            {
                sb.AppendLine("##### Response #####");
                sb.AppendLine(body);
            }
        }

        return sb.ToString();
    }

    public static string GetRequest(this ElasticsearchResponse elasticResponse, bool normalize = false, bool includeResponse = false, bool includeDebugInformation = false)
    {
        return GetErrorMessage(elasticResponse, null, normalize, includeResponse, includeDebugInformation);
    }

    public static async Task<bool> WaitForReadyAsync(this ElasticsearchClient client, CancellationToken cancellationToken, ILogger logger = null)
    {
        var nodes = client.ElasticsearchClientSettings.NodePool.Nodes.Select(n => n.Uri.ToString());
        var startTime = DateTime.UtcNow;

        while (!cancellationToken.IsCancellationRequested)
        {
            var pingResponse = await client.PingAsync(cancellationToken);
            if (pingResponse.IsValidResponse)
                return true;

            if (logger != null)
                logger?.LogInformation("Waiting for Elasticsearch to be ready {Server} after {Duration:g}...", nodes, DateTime.UtcNow.Subtract(startTime));

            await Task.Delay(1000, cancellationToken);
        }

        if (logger != null)
            logger?.LogError("Unable to connect to Elasticsearch {Server} after attempting for {Duration:g}", nodes, DateTime.UtcNow.Subtract(startTime));

        return false;
    }

    public static bool WaitForReady(this ElasticsearchClient client, CancellationToken cancellationToken, ILogger logger = null)
    {
        var nodes = client.ElasticsearchClientSettings.NodePool.Nodes.Select(n => n.Uri.ToString());
        var startTime = DateTime.UtcNow;

        while (!cancellationToken.IsCancellationRequested)
        {
            var pingResponse = client.Ping();
            if (pingResponse.IsValidResponse)
                return true;

            if (logger != null)
                logger?.LogInformation("Waiting for Elasticsearch to be ready {Server} after {Duration:g}...", nodes, DateTime.UtcNow.Subtract(startTime));

            Thread.Sleep(1000);
        }

        if (logger != null)
            logger?.LogError("Unable to connect to Elasticsearch {Server} after attempting for {Duration:g}", nodes, DateTime.UtcNow.Subtract(startTime));

        return false;
    }
}

internal class JsonUtility
{
    public static string Normalize(string jsonStr)
    {
        using var doc = JsonDocument.Parse(jsonStr);
        return Normalize(doc.RootElement);
    }

    public static string Normalize(JsonElement element)
    {
        var ms = new MemoryStream();
        var opts = new JsonWriterOptions
        {
            Indented = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };
        using (var writer = new Utf8JsonWriter(ms, opts))
        {
            Write(element, writer);
        }

        byte[] bytes = ms.ToArray();
        string str = Encoding.UTF8.GetString(bytes);
        return str;
    }

    private static void Write(JsonElement element, Utf8JsonWriter writer)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();

                foreach (var x in element.EnumerateObject().OrderBy(prop => prop.Name))
                {
                    writer.WritePropertyName(x.Name);
                    Write(x.Value, writer);
                }

                writer.WriteEndObject();
                break;

            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var x in element.EnumerateArray())
                {
                    Write(x, writer);
                }
                writer.WriteEndArray();
                break;

            case JsonValueKind.Number:
                writer.WriteNumberValue(element.GetDouble());
                break;

            case JsonValueKind.String:
                writer.WriteStringValue(element.GetString());
                break;

            case JsonValueKind.Null:
                writer.WriteNullValue();
                break;

            case JsonValueKind.True:
                writer.WriteBooleanValue(true);
                break;

            case JsonValueKind.False:
                writer.WriteBooleanValue(false);
                break;

            default:
                throw new NotImplementedException($"Kind: {element.ValueKind}");
        }
    }
}

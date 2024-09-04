using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class ElasticExtensions
{
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
    public static string GetErrorMessage(this IElasticsearchResponse elasticResponse, string message = null, bool normalize = false, bool includeResponse = false, bool includeDebugInformation = false)
    {
        if (elasticResponse == null)
            return String.Empty;

        var sb = new StringBuilder();

        if (!String.IsNullOrEmpty(message))
            sb.AppendLine(message);

        var response = elasticResponse as IResponse;
        if (includeDebugInformation && response?.DebugInformation != null)
            sb.AppendLine(response.DebugInformation);

        if (response?.OriginalException != null)
            sb.AppendLine($"Original: [{response.OriginalException.GetType().Name}] {response.OriginalException.Message}");

        if (response?.ServerError?.Error != null)
            sb.AppendLine($"Server Error (Index={response.ServerError.Error?.Index}): {response.ServerError.Error.Reason}");

        if (elasticResponse is BulkResponse bulkResponse)
            sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

        if (elasticResponse.ApiCall != null)
            sb.AppendLine($"[{elasticResponse.ApiCall.HttpStatusCode}] {elasticResponse.ApiCall.HttpMethod} {elasticResponse.ApiCall.Uri?.PathAndQuery}");

        if (elasticResponse.ApiCall?.RequestBodyInBytes != null)
        {
            string body = Encoding.UTF8.GetString(elasticResponse.ApiCall?.RequestBodyInBytes);
            if (normalize)
                body = JsonUtility.Normalize(body);
            sb.AppendLine(body);
        }

        var apiCall = response.ApiCall;
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

    public static string GetRequest(this IElasticsearchResponse elasticResponse, bool normalize = false, bool includeResponse = false, bool includeDebugInformation = false)
    {
        return GetErrorMessage(elasticResponse, null, normalize, includeResponse, includeDebugInformation);
    }

    public static async Task<bool> WaitForReadyAsync(this IElasticClient client, CancellationToken cancellationToken, ILogger logger = null)
    {
        var nodes = client.ConnectionSettings.ConnectionPool.Nodes.Select(n => n.Uri.ToString());
        var startTime = DateTime.UtcNow;

        while (!cancellationToken.IsCancellationRequested)
        {
            var pingResponse = await client.PingAsync(ct: cancellationToken);
            if (pingResponse.IsValid)
                return true;

            if (logger != null && logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
                logger?.LogInformation("Waiting for Elasticsearch to be ready {Server} after {Duration:g}...", nodes, DateTime.UtcNow.Subtract(startTime));

            await Task.Delay(1000, cancellationToken);
        }

        if (logger != null && logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Error))
            logger?.LogError("Unable to connect to Elasticsearch {Server} after attempting for {Duration:g}", nodes, DateTime.UtcNow.Subtract(startTime));

        return false;
    }

    public static bool WaitForReady(this IElasticClient client, CancellationToken cancellationToken, ILogger logger = null)
    {
        var nodes = client.ConnectionSettings.ConnectionPool.Nodes.Select(n => n.Uri.ToString());
        var startTime = DateTime.UtcNow;

        while (!cancellationToken.IsCancellationRequested)
        {
            var pingResponse = client.Ping();
            if (pingResponse.IsValid)
                return true;

            if (logger != null && logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
                logger?.LogInformation("Waiting for Elasticsearch to be ready {Server} after {Duration:g}...", nodes, DateTime.UtcNow.Subtract(startTime));

            Thread.Sleep(1000);
        }

        if (logger != null && logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Error))
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

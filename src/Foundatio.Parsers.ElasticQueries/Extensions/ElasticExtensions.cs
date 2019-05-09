using System;
using System.Linq;
using System.Text;
using System.Threading;
using Elasticsearch.Net;
using Microsoft.Extensions.Logging;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class ElasticExtensions {
        public static TermsInclude AddValue(this TermsInclude include, string value) {
            if (include?.Values == null)
                return new TermsInclude(new[] { value });

            var values = include.Values.ToList();
            values.Add(value);

            return new TermsInclude(values);
        } 

        public static TermsExclude AddValue(this TermsExclude exclude, string value) {
            if (exclude?.Values == null)
                return new TermsExclude(new[] { value });

            var values = exclude.Values.ToList();
            values.Add(value);
            
            return new TermsExclude(values);
        } 

        // TODO: Handle IFailureReason/BulkIndexByScrollFailure and other bulk response types.
        public static string GetErrorMessage(this IElasticsearchResponse response) {
            if (response == null)
                return String.Empty;
            
            var sb = new StringBuilder();

            var apiCall = response.ApiCall;
            if (apiCall.OriginalException != null)
                sb.AppendLine($"Original: ({apiCall.HttpStatusCode} - {apiCall.OriginalException.GetType().Name}) {apiCall.OriginalException.Message}");
            
            if (response is IResponse responseWithError && responseWithError.ServerError != null)
                sb.AppendLine($"Server: ({responseWithError.ServerError.Status}) {responseWithError.ServerError.Error}");

            if (response is BulkResponse bulkResponse)
                sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

        public static string GetRequest(this IElasticsearchResponse response, bool normalize = false, bool includeResponse = false, bool includeDebugInformation = false) {
            if (response == null)
                return String.Empty;

            var sb = new StringBuilder();
            var responseWithError = response as IResponse;
            if (includeDebugInformation && responseWithError?.DebugInformation != null)
                sb.AppendLine(responseWithError.DebugInformation);

            var apiCall = response.ApiCall;
            if (apiCall.HttpStatusCode.HasValue) {
                sb.Append(response.ApiCall.HttpStatusCode);
                sb.Append(" ");
            }
            sb.Append(apiCall.HttpMethod);
            sb.Append(" ");
            sb.AppendLine(apiCall.Uri.ToString());

            if (apiCall.RequestBodyInBytes != null) {
                string body = Encoding.UTF8.GetString(apiCall.RequestBodyInBytes)?.Trim();
                
                if (normalize)
                    body = JsonUtility.NormalizeJsonString(body)?.Trim();
                
                if (!String.IsNullOrWhiteSpace(body))
                    sb.AppendLine(body);
            }

            if (includeResponse && apiCall.ResponseBodyInBytes != null && apiCall.ResponseBodyInBytes.Length > 0 && apiCall.ResponseBodyInBytes.Length < 20000) {
                string responseData = Encoding.UTF8.GetString(apiCall.ResponseBodyInBytes)?.Trim();
                
                if (!String.IsNullOrWhiteSpace(responseData)) {
                    sb.AppendLine("##### Response #####");
                    sb.AppendLine(responseData);
                }
            }

            return sb.ToString();
        }

        public static bool WaitForReady(this IElasticClient client, CancellationToken cancellationToken, ILogger logger = null) {
            var nodes = client.ConnectionSettings.ConnectionPool.Nodes.Select(n => n.Uri.ToString());
            var startTime = DateTime.UtcNow;

            while (!cancellationToken.IsCancellationRequested) {
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

    internal class JsonUtility {
        public static string NormalizeJsonString(string json) {
            var parsedObject = JObject.Parse(json);
            var normalizedObject = SortPropertiesAlphabetically(parsedObject);
            return JsonConvert.SerializeObject(normalizedObject, Formatting.Indented);
        }

        private static JObject SortPropertiesAlphabetically(JObject original) {
            var result = new JObject();

            foreach (var property in original.Properties().ToList().OrderBy(p => p.Name)) {
                if (property.Value is JObject value) {
                    value = SortPropertiesAlphabetically(value);
                    result.Add(property.Name, value);
                } else if (property.Value is JArray array) {
                    array = SortArrayAlphabetically(array);
                    result.Add(property.Name, array);
                } else {
                    result.Add(property.Name, property.Value);
                }
            }

            return result;
        }

        private static JArray SortArrayAlphabetically(JArray original) {
            var result = new JArray();

            foreach (var item in original) {
                if (item is JObject value)
                    result.Add(SortPropertiesAlphabetically(value));
                else if (item is JArray array)
                    result.Add(SortArrayAlphabetically(array));
                else
                    result.Add(item);
            }

            return result;
        }
    }
}

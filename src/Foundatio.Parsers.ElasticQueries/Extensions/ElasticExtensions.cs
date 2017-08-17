using System;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class ElasticExtensions {
        public static string GetErrorMessage(this IApiCallDetails response) {
            var sb = new StringBuilder();

            if (response.OriginalException != null)
                sb.AppendLine($"Original: ({response.HttpStatusCode} - {response.OriginalException.GetType().Name}) {response.OriginalException.Message}");

            if (response.ServerError != null)
                sb.AppendLine($"Server: ({response.ServerError.Status}) {response.ServerError.Error}");

            var bulkResponse = response as IBulkResponse;
            if (bulkResponse != null)
                sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

        // TODO: Handle IFailureReason/BulkIndexByScrollFailure and other bulk response types.
        public static string GetErrorMessage(this IResponse response) {
            var sb = new StringBuilder();

            if (response.OriginalException != null)
                sb.AppendLine($"Original: ({response.ApiCall.HttpStatusCode} - {response.OriginalException.GetType().Name}) {response.OriginalException.Message}");

            if (response.ServerError != null)
                sb.AppendLine($"Server: ({response.ServerError.Status}) {response.ServerError.Error}");

            var bulkResponse = response as IBulkResponse;
            if (bulkResponse != null)
                sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

        public static string GetRequest(this IApiCallDetails response, bool normalize = false) {
            if (response == null)
                return String.Empty;

            if (response.RequestBodyInBytes != null) {
                string body = Encoding.UTF8.GetString(response.RequestBodyInBytes);
                if (normalize)
                    body = JsonUtility.NormalizeJsonString(body);
                return $"{response.HttpMethod} {response.Uri.PathAndQuery}\r\n{body}\r\n";
            } else {
                return $"{response.HttpMethod} {response.Uri.PathAndQuery}\r\n";
            }
        }

        public static string GetRequest(this IResponse response, bool normalize = false) {
            return GetRequest(response?.ApiCall, normalize);
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

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

            if (response is IBulkResponse bulkResponse)
                sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

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
        public static string GetErrorMessage(this IResponse response) {
            var sb = new StringBuilder();

            if (response.OriginalException != null)
                sb.AppendLine($"Original: ({response.ApiCall.HttpStatusCode} - {response.OriginalException.GetType().Name}) {response.OriginalException.Message}");

            if (response.ServerError != null)
                sb.AppendLine($"Server: ({response.ServerError.Status}) {response.ServerError.Error}");

            if (response is IBulkResponse bulkResponse)
                sb.AppendLine($"Bulk: {String.Join("\r\n", bulkResponse.ItemsWithErrors.Select(i => i.Error))}");

            if (sb.Length == 0)
                sb.AppendLine("Unknown error.");

            return sb.ToString();
        }

        public static string GetRequest(this IApiCallDetails response, bool normalize = false, bool includeResponse = false, bool includeDebugInformation = false) {
            if (response == null)
                return String.Empty;

            var sb = new StringBuilder();
            if (includeDebugInformation && response.DebugInformation != null)
                sb.AppendLine(response.DebugInformation);
            if (response.HttpStatusCode.HasValue) {
                sb.Append(response.HttpStatusCode);
                sb.Append(" ");
            }
            sb.Append(response.HttpMethod);
            sb.Append(" ");
            sb.AppendLine(response.Uri.PathAndQuery);

            if (response.RequestBodyInBytes != null) {
                string body = Encoding.UTF8.GetString(response.RequestBodyInBytes)?.Trim();
                
                if (normalize)
                    body = JsonUtility.NormalizeJsonString(body)?.Trim();
                
                if (!String.IsNullOrWhiteSpace(body))
                    sb.AppendLine(body);
            }

            if (includeResponse && response.ResponseBodyInBytes != null && response.ResponseBodyInBytes.Length > 0 && response.ResponseBodyInBytes.Length < 20000) {
                string responseData = Encoding.UTF8.GetString(response.ResponseBodyInBytes)?.Trim();
                
                if (!String.IsNullOrWhiteSpace(responseData)) {
                    sb.AppendLine("##### Response #####");
                    sb.AppendLine(responseData);
                }
            }

            return sb.ToString();
        }

        public static string GetRequest(this IResponse response, bool normalize = false, bool includeResponse = false, bool includeDebugInformation = false) {
            return GetRequest(response?.ApiCall, normalize, includeResponse, includeDebugInformation);
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

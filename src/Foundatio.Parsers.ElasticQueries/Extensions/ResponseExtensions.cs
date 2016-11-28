using Elasticsearch.Net;
using System;
using System.Text;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class ResponseExtensions {
        public static string GetRequest(this IResponseWithRequestInformation response) {
            var requestUrl = new Uri(response.RequestInformation.RequestUrl);
            return $"{response.RequestInformation.RequestMethod.ToUpper()} {requestUrl.PathAndQuery}\r\n{Encoding.UTF8.GetString(response.RequestInformation.Request)}\r\n";
        }
    }
}

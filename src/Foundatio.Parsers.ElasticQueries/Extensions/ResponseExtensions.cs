using System;
using System.Text;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class ResponseExtensions {
        public static string GetRequest(this IResponse response) {
            return response.ApiCall.RequestBodyInBytes != null ?
                $"{response.ApiCall.HttpMethod} {response.ApiCall.Uri.PathAndQuery}\r\n{Encoding.UTF8.GetString(response.ApiCall.RequestBodyInBytes)}\r\n"
                : $"{response.ApiCall.HttpMethod} {response.ApiCall.Uri.PathAndQuery}\r\n";
        }
    }
}

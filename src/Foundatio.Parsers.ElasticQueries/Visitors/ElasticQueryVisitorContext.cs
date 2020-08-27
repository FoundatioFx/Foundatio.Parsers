using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticQueryVisitorContext : QueryVisitorContext, IQueryVisitorContextWithIncludeResolver, IQueryVisitorContextWithFieldResolver, IElasticQueryVisitorContext, IQueryVisitorContextWithValidator {
        public string DefaultTimeZone { get; set; }
        public bool UseScoring { get; set; }
        public ElasticMappingResolver MappingResolver { get; set; }
        public IncludeResolver IncludeResolver { get; set; }
        public QueryFieldResolver FieldResolver { get; set; }
        public Func<QueryValidationInfo, Task<bool>> Validator { get; set; }
        public QueryValidationInfo ValidationInfo { get; set; }
    }
}

using System;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticQueryVisitorContext : QueryVisitorContext, IQueryVisitorContextWithIncludeResolver, IQueryVisitorContextWithFieldResolver, IElasticQueryVisitorContext, IQueryVisitorContextWithValidation {
        public Lazy<string> DefaultTimeZone { get; set; }
        public bool UseScoring { get; set; }
        public ElasticMappingResolver MappingResolver { get; set; }
    }
}

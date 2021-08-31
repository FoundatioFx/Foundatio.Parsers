using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public interface IElasticQueryVisitorContext : IQueryVisitorContext {
        Func<Task<string>> DefaultTimeZone { get; set; }
        bool UseScoring { get; set; }
        ElasticMappingResolver MappingResolver { get; set; }
        ICollection<ElasticRuntimeField> RuntimeFields { get; }
        RuntimeFieldResolver RuntimeFieldResolver { get; set; }
    }
}

namespace Foundatio.Parsers {
    public delegate Task<ElasticRuntimeField> RuntimeFieldResolver(string field);

    public class ElasticRuntimeField {
        public string Name { get; set; }
        public ElasticRuntimeFieldType FieldType { get; set; } = ElasticRuntimeFieldType.Keyword;
        public string Script { get; set; }
    }

    // This is the list of supported field types for runtime fields:
    // https://www.elastic.co/guide/en/elasticsearch/reference/master/runtime-mapping-fields.html
    public enum ElasticRuntimeFieldType {
        Boolean,
        Date,
        Double,
        GeoPoint,
        Ip,
        Keyword,
        Long
    }
}
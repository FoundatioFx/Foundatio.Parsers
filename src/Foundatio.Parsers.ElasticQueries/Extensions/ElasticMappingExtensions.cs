using System;
using System.Collections.Generic;
using System.Text;

namespace Nest {
    public static class ElasticMapping {

        /// <summary>
        /// Not chainable with AddSortField. Use AddKeywordAndSortFields to add both.
        /// </summary>
        public static TextPropertyDescriptor<T> AddKeywordField<T>(this TextPropertyDescriptor<T> descriptor) where T : class {
            return descriptor.Fields(f => f.Keyword(s => s.Name(KeywordFieldName).IgnoreAbove(256)));
        }

        /// <summary>
        /// Not chainable with AddKeywordField. Use AddKeywordAndSortFields to add both.
        /// </summary>
        public static TextPropertyDescriptor<T> AddSortField<T>(this TextPropertyDescriptor<T> descriptor, string normalizer = "sort") where T : class {
            return descriptor.Fields(f => f.Keyword(s => s.Name(ElasticMapping.SortFieldName).Normalizer(normalizer).IgnoreAbove(256)));
        }

        public static TextPropertyDescriptor<T> AddKeywordAndSortFields<T>(this TextPropertyDescriptor<T> descriptor, string sortNormalizer = "sort") where T : class {
            return descriptor.Fields(f => f.Keyword(s => s.Name(ElasticMapping.KeywordFieldName).IgnoreAbove(256)).Keyword(s => s.Name(SortFieldName).Normalizer(sortNormalizer).IgnoreAbove(256)));
        }

        public static AnalysisDescriptor AddSortNormalizer(this AnalysisDescriptor descriptor) {
            return descriptor.Normalizers(d => d.Custom("sort", n => n.Filters("lowercase", "asciifolding")));
        }

        public static string KeywordFieldName = "keyword";
        public static string SortFieldName = "sort";
    }
}

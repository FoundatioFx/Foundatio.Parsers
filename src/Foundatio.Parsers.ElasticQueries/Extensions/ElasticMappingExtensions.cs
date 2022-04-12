namespace Nest;

public static class ElasticMapping {
    /// <summary>
    /// Not chainable with AddSortField. Use AddKeywordAndSortFields to add both.
    /// </summary>
    public static TextPropertyDescriptor<T> AddKeywordField<T>(this TextPropertyDescriptor<T> descriptor) where T : class {
        return descriptor.AddKeywordField(null);
    }

    /// <summary>
    /// Not chainable with AddSortField. Use AddKeywordAndSortFields to add both.
    /// </summary>
    public static TextPropertyDescriptor<T> AddKeywordField<T>(this TextPropertyDescriptor<T> descriptor, string normalizer) where T : class {
        return descriptor.Fields(f => f.Keyword(s => s.Name(KeywordFieldName).Normalizer(normalizer).IgnoreAbove(256)));
    }

    /// <summary>
    /// Not chainable with AddSortField. Use AddKeywordAndSortFields to add both.
    /// </summary>
    public static TextPropertyDescriptor<T> AddKeywordField<T>(this TextPropertyDescriptor<T> descriptor, bool lowercase) where T : class {
        return descriptor.AddKeywordField(lowercase ? "lowercase" : null);
    }

    /// <summary>
    /// Not chainable with AddKeywordField. Use AddKeywordAndSortFields to add both.
    /// </summary>
    public static TextPropertyDescriptor<T> AddSortField<T>(this TextPropertyDescriptor<T> descriptor, string normalizer = "sort") where T : class {
        return descriptor.Fields(f => f.Keyword(s => s.Name(SortFieldName).Normalizer(normalizer).IgnoreAbove(256)));
    }

    public static TextPropertyDescriptor<T> AddKeywordAndSortFields<T>(this TextPropertyDescriptor<T> descriptor, string sortNormalizer = "sort", string keywordNormalizer = null) where T : class {
        return descriptor.Fields(f => f
            .Keyword(s => s.Name(KeywordFieldName).Normalizer(keywordNormalizer).IgnoreAbove(256))
            .Keyword(s => s.Name(SortFieldName).Normalizer(sortNormalizer).IgnoreAbove(256)));
    }

    public static TextPropertyDescriptor<T> AddKeywordAndSortFields<T>(this TextPropertyDescriptor<T> descriptor, bool keywordLowercase) where T : class {
        return descriptor.AddKeywordAndSortFields(keywordNormalizer: keywordLowercase ? "lowercase" : null);
    }

    public static AnalysisDescriptor AddSortNormalizer(this AnalysisDescriptor descriptor) {
        return descriptor.Normalizers(d => d.Custom("sort", n => n.Filters("lowercase", "asciifolding")));
    }

    public static string KeywordFieldName = "keyword";
    public static string SortFieldName = "sort";
}

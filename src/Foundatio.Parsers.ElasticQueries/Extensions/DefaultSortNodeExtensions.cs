using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class DefaultSortNodeExtensions {
        public static IFieldSort GetDefaultSort(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            string field = elasticContext.MappingResolver.GetNonAnalyzedFieldName(node.Field, "sort");
            var fieldType = elasticContext.MappingResolver.GetFieldType(field);

            var sort = new FieldSort {
                Field = field,
                UnmappedType = fieldType == FieldType.None ? FieldType.Keyword : fieldType,
                Order = node.IsNodeOrGroupNegated() ? SortOrder.Descending : SortOrder.Ascending
            };
            
            return sort;
        }
    }
}

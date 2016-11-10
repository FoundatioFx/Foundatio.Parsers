using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class GetSortFieldsVisitor : QueryNodeVisitorWithResultBase<IEnumerable<IFieldSort>> {
        private readonly List<IFieldSort> _fields = new List<IFieldSort>();

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Field))
                return;

            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            string field = elasticContext.GetNonAnalyzedFieldName(node.Field);

            var sort = new Sort { Field = field };
            if (node.IsNodeNegated())
                sort.Order = SortOrder.Descending;

            _fields.Add(sort);
        }

        public override IEnumerable<IFieldSort> Accept(IQueryNode node, IQueryVisitorContext context) {
            node.Accept(this, context);
            return _fields;
        }

        public static IEnumerable<IFieldSort> Run(IQueryNode node, IQueryVisitorContext context = null) {
            return new GetSortFieldsVisitor().Accept(node, context);
        }
    }
}

using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class NestedVisitor: ChainableQueryVisitor {
        public override Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Field) || !IsNestedPropertyType(node.GetNameParts(), context))
                return base.VisitAsync(node, context);

            node.SetQuery(new NestedQuery { Path = node.GetFullName() });
            return base.VisitAsync(node, context);
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (!IsNestedPropertyType(node.Field?.Split('.'), context))
                return;

            node.SetQuery(new NestedQuery { Path = node.GetParentFullName(), Query = node.GetQuery(() => node.GetDefaultQuery(context)) });
        }

        private bool IsNestedPropertyType(string[] nameParts, IQueryVisitorContext context) {
            if (nameParts == null || !(context is IElasticQueryVisitorContext elasticContext) || nameParts.Length == 0)
                return false;

            string fieldName = String.Empty;
            for (int i = 0; i < nameParts.Length; i++) {
                if (i > 0)
                    fieldName += ".";

                fieldName += nameParts[i];

                if (elasticContext.IsNestedPropertyType(fieldName))
                    return true;
            }

            return false;
        }
    }
}

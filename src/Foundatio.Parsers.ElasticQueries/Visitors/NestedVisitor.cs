using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class NestedVisitor: ChainableQueryVisitor {
        public override void Visit(TermNode node, IQueryVisitorContext context) {
            var nestedProperty = GetNestedProperty(node.Field, context);
            if (nestedProperty == null)
                return;

            node.SetQuery(new NestedQuery { Path = nestedProperty, Query = node.GetQuery(() => node.GetDefaultQuery(context)) });
        }

        private string GetNestedProperty(string fullName, IQueryVisitorContext context) {
            string[] nameParts = fullName?.Split('.').ToArray();
            
            if (nameParts == null || !(context is IElasticQueryVisitorContext elasticContext) || nameParts.Length == 0)
                return null;

            string fieldName = String.Empty;
            for (int i = 0; i < nameParts.Length; i++) {
                if (i > 0)
                    fieldName += ".";

                fieldName += nameParts[i];

                if (elasticContext.IsNestedPropertyType(fieldName))
                    return fieldName;
            }

            return null;
        }
    }
}

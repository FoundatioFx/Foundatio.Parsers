using System;
using System.Linq;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class ElasticValidationVisitor : ValidationVisitor {
        protected override bool CanResolveField(string field, IQueryVisitorContext context) {
            if (context is not IElasticQueryVisitorContext elasticContext)
                throw new InvalidOperationException("Context needs to be of type IElasticQueryVisitorContext in order to use the ElasticValidationVisitor.");

            var resolvedField = elasticContext.MappingResolver.GetMapping(field);
            if (resolvedField.Found)
                return true;

            if (elasticContext.RuntimeFields.Any(f => f.Name.Equals(field, StringComparison.OrdinalIgnoreCase)))
                return true;

            if (elasticContext.RuntimeFieldResolver != null && elasticContext.RuntimeFieldResolver(field) != null)
                return true;

            return false;
        }
    }
}

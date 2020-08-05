using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class QueryVisitorContextExtensions {
        public static T SetMappingResolver<T>(this T context, ElasticMappingResolver mappingResolver) where T: IQueryVisitorContext {
            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            elasticContext.MappingResolver = mappingResolver ?? ElasticMappingResolver.NullInstance;

            return context;
        }
        public static ElasticMappingResolver GetMappingResolver<T>(this T context) where T : IQueryVisitorContext {
            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            return elasticContext.MappingResolver ?? ElasticMappingResolver.NullInstance;
        }


        public static T SetDefaultOperator<T>(this T context, Operator defaultOperator) where T : IQueryVisitorContext {
            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            elasticContext.DefaultOperator = defaultOperator;

            return context;
        }

        public static T UseScoring<T>(this T context) where T : IQueryVisitorContext {
            if (!(context is IElasticQueryVisitorContext elasticContext))
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            elasticContext.UseScoring = true;

            return context;
        }
    }
}
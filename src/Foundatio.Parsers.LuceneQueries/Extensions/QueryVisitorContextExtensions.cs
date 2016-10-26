using System;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Extensions {
    public static class QueryVisitorContextExtensions {
         public static AliasResolver GetRootAliasResolver(this IQueryVisitorContext context) {
            var aliasContext = context as IQueryVisitorContextWithAliasResolver;
            if (aliasContext == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithAliasResolver", nameof(context));

            return aliasContext.RootAliasResolver;
        }
        
        public static T SetRootAliasResolver<T>(this T context, AliasResolver aliasResolver) where T: IQueryVisitorContext {
            var aliasContext = context as IQueryVisitorContextWithAliasResolver;
            if (aliasContext == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithAliasResolver", nameof(context));

            aliasContext.RootAliasResolver = aliasResolver;

            return context;
        }
    }
}
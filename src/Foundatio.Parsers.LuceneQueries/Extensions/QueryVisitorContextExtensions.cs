using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using System.Threading.Tasks;

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

        public static Func<string, Task<string>> GetIncludeResolver(this IQueryVisitorContext context) {
            var aliasContext = context as IQueryVisitorContextWithIncludeResolver;
            if (aliasContext == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithIncludeResolver", nameof(context));

            return aliasContext.IncludeResolver;
        }

        public static T SetIncludeResolver<T>(this T context, Func<string, Task<string>> includeResolver) where T : IQueryVisitorContext {
            var aliasContext = context as IQueryVisitorContextWithIncludeResolver;
            if (aliasContext == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithIncludeResolver", nameof(context));

            aliasContext.IncludeResolver = includeResolver;

            return context;
        }
    }
}
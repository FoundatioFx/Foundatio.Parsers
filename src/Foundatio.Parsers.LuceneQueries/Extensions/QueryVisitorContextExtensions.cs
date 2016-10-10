using System;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public static class QueryVisitorContextExtensions {
        private const string RootAliasResolverKey = "@AliasResolver";
        public static AliasResolver GetRootAliasResolver(this IQueryVisitorContext context) {
            object value = null;
            if (!context.Data.TryGetValue(RootAliasResolverKey, out value))
                return null;

            return value as AliasResolver;
        }
        
        public static T SetRootAliasResolver<T>(this T context, AliasResolver aliasResolver) where T: IQueryVisitorContext {
            if (aliasResolver == null)
                throw new ArgumentNullException(nameof(aliasResolver));

            context.Data[RootAliasResolverKey] = aliasResolver;

            return context;
        }

        public static T SetRootAliasMap<T>(this T context, AliasMap aliasMap) where T : IQueryVisitorContext {
            if (aliasMap == null)
                throw new ArgumentNullException(nameof(aliasMap));

            context.Data[RootAliasResolverKey] = (AliasResolver)aliasMap.Resolve;

            return context;
        }

        public static void RemoveRootAliasResolver(this IQueryVisitorContext context) {
            if (context.Data.ContainsKey(RootAliasResolverKey))
                context.Data.Remove(RootAliasResolverKey);
        }
    }
}
using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Extensions {
    public static class QueryVisitorContextExtensions {
        public static QueryFieldResolver GetFieldResolver(this IQueryVisitorContext context) {
            var resolverContext = context as IQueryVisitorContextWithFieldResolver;
            return resolverContext?.FieldResolver;
        }

        public static T SetFieldResolver<T>(this T context, QueryFieldResolver resolver) where T: IQueryVisitorContext {
            if (context is not IQueryVisitorContextWithFieldResolver resolverContext)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithFieldResolver", nameof(context));

            resolverContext.FieldResolver = resolver;

            return context;
        }

        public static IncludeResolver GetIncludeResolver(this IQueryVisitorContext context) {
            if (context is not IQueryVisitorContextWithIncludeResolver includeContext)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithIncludeResolver", nameof(context));

            return includeContext.IncludeResolver;
        }

        public static T SetIncludeResolver<T>(this T context, IncludeResolver includeResolver) where T : IQueryVisitorContext {
            if (context is not IQueryVisitorContextWithIncludeResolver includeContext)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithIncludeResolver", nameof(context));

            includeContext.IncludeResolver = includeResolver;

            return context;
        }

        public static QueryValidationOptions GetValidationOptions(this IQueryVisitorContext context) {
            if (context is not IQueryVisitorContextWithValidation validatorContext)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidation", nameof(context));

            return validatorContext.ValidationOptions;
        }

        public static T SetValidationOptions<T>(this T context, QueryValidationOptions options) where T : IQueryVisitorContext {
            if (context is not IQueryVisitorContextWithValidation validatorContext)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidation", nameof(context));

            validatorContext.ValidationOptions = options;

            return context;
        }

        public static QueryValidationInfo GetValidationInfo(this IQueryVisitorContext context) {
            if (context is not IQueryVisitorContextWithValidation validatorContext)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidation", nameof(context));

            return validatorContext.ValidationInfo ??= new QueryValidationInfo();
        }

        public static T SetValidationInfo<T>(this T context, QueryValidationInfo validationInfo) where T : IQueryVisitorContext {
            if (context is not IQueryVisitorContextWithValidation validatorContext)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidation", nameof(context));

            validatorContext.ValidationInfo = validationInfo;

            return context;
        }

        public static T SetDefaultFields<T>(this T context, string[] defaultFields) where T : IQueryVisitorContext {
            context.DefaultFields = defaultFields;

            return context;
        }


        public static T SetDefaultOperator<T>(this T context, GroupOperator defaultOperator) where T : IQueryVisitorContext {
            context.DefaultOperator = defaultOperator;

            return context;
        }

        public static T SetValue<T>(this T context, string key, object value) where T : IQueryVisitorContext {
            context.Data[key] = value;

            return context;
        }

        public static T GetValue<T>(this IQueryVisitorContext context, string key) {
            if (context.Data.TryGetValue(key, out var value) && value is T typedValue)
                return typedValue;

            return default;
        }

        public static DateTime? GetDate(this IQueryVisitorContext context, string key) {
            if (context.Data.TryGetValue(key, out var value) && value is DateTime date)
                return date;

            return null;
        }

        public static string GetString(this IQueryVisitorContext context, string key) {
            if (context.Data.TryGetValue(key, out var value) && value is string str)
                return str;

            return null;
        }

        public static bool GetBoolean(this IQueryVisitorContext context, string key, bool defaultValue = false) {
            if (context.Data.TryGetValue(key, out var value) && value is bool b)
                return b;

            return defaultValue;
        }

        private const string AlternateInvertedCriteriaKey = "AlternateInvertedCriteria";
        public static T SetAlternateInvertedCriteria<T>(this T context, IQueryNode criteria) where T : IQueryVisitorContext {
            context.Data[AlternateInvertedCriteriaKey] = criteria;

            return context;
        }

        public static IQueryNode GetAlternateInvertedCriteria(this IQueryVisitorContext context) {
            return context.GetValue<IQueryNode>(AlternateInvertedCriteriaKey);
        }
    }
}
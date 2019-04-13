using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using System.Threading.Tasks;

namespace Foundatio.Parsers.LuceneQueries.Extensions {
    public static class QueryVisitorContextExtensions {
         public static QueryFieldResolver GetDynamicFieldResolver(this IQueryVisitorContext context) {
            var aliasContext = context as IQueryVisitorContextWithFieldResolver;
            if (aliasContext == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithDynamicFields", nameof(context));

            return aliasContext.FieldResolver;
        }

        public static T SetDynamicFieldResolver<T>(this T context, QueryFieldResolver resolver) where T: IQueryVisitorContext {
            var aliasContext = context as IQueryVisitorContextWithFieldResolver;
            if (aliasContext == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithDynamicFields", nameof(context));

            aliasContext.FieldResolver = resolver;

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

        public static Func<QueryValidationInfo, Task<bool>> GetValidator(this IQueryVisitorContext context) {
            var contextWithValidator = context as IQueryVisitorContextWithValidator;
            if (contextWithValidator == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidator", nameof(context));

            return contextWithValidator.Validator;
        }

        public static T SetValidator<T>(this T context, Func<QueryValidationInfo, Task<bool>> validator) where T : IQueryVisitorContext {
            var contextWithValidator = context as IQueryVisitorContextWithValidator;
            if (contextWithValidator == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidator", nameof(context));

            contextWithValidator.Validator = validator;

            return context;
        }

        public static QueryValidationInfo GetValidationInfo(this IQueryVisitorContext context) {
            var contextWithValidator = context as IQueryVisitorContextWithValidator;
            if (contextWithValidator == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidator", nameof(context));

            if (contextWithValidator.ValidationInfo == null)
                contextWithValidator.ValidationInfo = new QueryValidationInfo();

            return contextWithValidator.ValidationInfo;
        }

        public static T SetValidationInfo<T>(this T context, QueryValidationInfo validationInfo) where T : IQueryVisitorContext {
            var contextWithValidator = context as IQueryVisitorContextWithValidator;
            if (contextWithValidator == null)
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithAliasResolver", nameof(context));

            contextWithValidator.ValidationInfo = validationInfo;

            return context;
        }
    }
}
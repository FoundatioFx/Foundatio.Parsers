using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Extensions {
    public static class QueryVisitorContextExtensions {
         public static QueryFieldResolver GetFieldResolver(this IQueryVisitorContext context) {
            var resolverContext = context as IQueryVisitorContextWithFieldResolver;
            return resolverContext?.FieldResolver;
         }

        public static T SetFieldResolver<T>(this T context, QueryFieldResolver resolver) where T: IQueryVisitorContext {
            if (!(context is IQueryVisitorContextWithFieldResolver resolverContext))
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithFieldResolver", nameof(context));

            resolverContext.FieldResolver = resolver;

            return context;
        }

        public static IncludeResolver GetIncludeResolver(this IQueryVisitorContext context) {
            if (!(context is IQueryVisitorContextWithIncludeResolver includeContext))
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithIncludeResolver", nameof(context));

            return includeContext.IncludeResolver;
        }

        public static T SetIncludeResolver<T>(this T context, IncludeResolver includeResolver) where T : IQueryVisitorContext {
            if (!(context is IQueryVisitorContextWithIncludeResolver includeContext))
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithIncludeResolver", nameof(context));

            includeContext.IncludeResolver = includeResolver;

            return context;
        }

        public static Func<QueryValidationInfo, Task<bool>> GetValidator(this IQueryVisitorContext context) {
            if (!(context is IQueryVisitorContextWithValidator validatorContext))
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidator", nameof(context));

            return validatorContext.Validator;
        }

        public static T SetValidator<T>(this T context, Func<QueryValidationInfo, Task<bool>> validator) where T : IQueryVisitorContext {
            if (!(context is IQueryVisitorContextWithValidator validatorContext))
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidator", nameof(context));

            validatorContext.Validator = validator;

            return context;
        }

        public static QueryValidationInfo GetValidationInfo(this IQueryVisitorContext context) {
            if (!(context is IQueryVisitorContextWithValidator validatorContext))
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithValidator", nameof(context));

            return validatorContext.ValidationInfo ?? (validatorContext.ValidationInfo = new QueryValidationInfo());
        }

        public static T SetValidationInfo<T>(this T context, QueryValidationInfo validationInfo) where T : IQueryVisitorContext {
            if (!(context is IQueryVisitorContextWithValidator validatorContext))
                throw new ArgumentException("Context must be of type IQueryVisitorContextWithAliasResolver", nameof(context));

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
    }
}
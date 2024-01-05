using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Extensions;

public static class QueryVisitorContextExtensions
{
    public static QueryFieldResolver GetFieldResolver(this IQueryVisitorContext context)
    {
        var resolverContext = context as IQueryVisitorContextWithFieldResolver;
        return resolverContext?.FieldResolver;
    }

    public static T SetFieldResolver<T>(this T context, Func<string, string> resolver) where T : IQueryVisitorContext
    {
        if (context is not IQueryVisitorContextWithFieldResolver resolverContext)
            throw new ArgumentException("Context must be of type IQueryVisitorContextWithFieldResolver", nameof(context));

        resolverContext.FieldResolver = (field, _) => Task.FromResult(resolver(field));

        return context;
    }

    public static T SetFieldResolver<T>(this T context, QueryFieldResolver resolver) where T : IQueryVisitorContext
    {
        if (context is not IQueryVisitorContextWithFieldResolver resolverContext)
            throw new ArgumentException("Context must be of type IQueryVisitorContextWithFieldResolver", nameof(context));

        resolverContext.FieldResolver = resolver;

        return context;
    }

    public static IncludeResolver GetIncludeResolver(this IQueryVisitorContext context)
    {
        if (context is not IQueryVisitorContextWithIncludeResolver includeContext)
            throw new ArgumentException("Context must be of type IQueryVisitorContextWithIncludeResolver", nameof(context));

        return includeContext.IncludeResolver;
    }

    public static T SetIncludeResolver<T>(this T context, IncludeResolver includeResolver) where T : IQueryVisitorContext
    {
        if (context is not IQueryVisitorContextWithIncludeResolver includeContext)
            throw new ArgumentException("Context must be of type IQueryVisitorContextWithIncludeResolver", nameof(context));

        includeContext.IncludeResolver = includeResolver;

        return context;
    }

    private const string IncludeStackKey = "@IncludeStackKey";
    internal static Stack<string> GetIncludeStack(this IQueryVisitorContext context)
    {
        var includeStack = context.GetValue<Stack<string>>(IncludeStackKey);
        if (includeStack != null)
            return includeStack;

        includeStack = new Stack<string>();
        context.SetValue(IncludeStackKey, includeStack);

        return includeStack;
    }

    private const string ValidationOptionsKey = "@ValidationOptions";
    public static QueryValidationOptions GetValidationOptions(this IQueryVisitorContext context)
    {
        if (context is not IQueryVisitorContextWithValidation validatorContext)
        {
            var validationOptions = context.GetValue<QueryValidationOptions>(ValidationOptionsKey);
            if (validationOptions == null)
            {
                validationOptions = new QueryValidationOptions();
                context.SetValue(ValidationOptionsKey, validationOptions);
            }

            return validationOptions;
        }

        return validatorContext.ValidationOptions ??= new QueryValidationOptions();
    }

    public static bool HasValidationOptions(this IQueryVisitorContext context)
    {
        if (context is not IQueryVisitorContextWithValidation validatorContext)
            return context.Data.ContainsKey(ValidationOptionsKey);

        return validatorContext.ValidationOptions != null;
    }

    public static T SetValidationOptions<T>(this T context, QueryValidationOptions options) where T : IQueryVisitorContext
    {
        if (options == null)
            return context;

        if (context is not IQueryVisitorContextWithValidation validatorContext)
        {
            context.SetValue(ValidationOptionsKey, options);
            return context;
        }

        validatorContext.ValidationOptions = options;

        return context;
    }

    private const string ValidationResultKey = "@ValidationResult";
    public static QueryValidationResult GetValidationResult(this IQueryVisitorContext context)
    {
        if (context is not IQueryVisitorContextWithValidation validatorContext)
        {
            var validationResult = context.GetValue<QueryValidationResult>(ValidationResultKey);
            if (validationResult == null)
            {
                validationResult = new QueryValidationResult();
                context.SetValue(ValidationResultKey, validationResult);
            }

            return validationResult;
        }

        return validatorContext.ValidationResult ??= new QueryValidationResult();
    }

    public static void ThrowIfInvalid(this IQueryVisitorContext context)
    {
        var validationResult = context.GetValidationResult();
        if (!validationResult.IsValid)
            throw new QueryValidationException($"Invalid query: {validationResult.Message}", validationResult);
    }

    public static void AddValidationError(this IQueryVisitorContext context, string message, int index = -1)
    {
        context.GetValidationResult().ValidationErrors.Add(new QueryValidationError(message, index));
    }

    public static bool IsValid(this IQueryVisitorContext context)
    {
        return context.GetValidationResult().IsValid;
    }

    public static ICollection<QueryValidationError> GetValidationErrors(this IQueryVisitorContext context)
    {
        return context.GetValidationResult().ValidationErrors ?? new List<QueryValidationError>();
    }

    public static string GetValidationMessage(this IQueryVisitorContext context)
    {
        return context.GetValidationResult().Message;
    }

    public static T SetDefaultFields<T>(this T context, string[] defaultFields) where T : IQueryVisitorContext
    {
        context.DefaultFields = defaultFields;

        return context;
    }

    public static T SetDefaultOperator<T>(this T context, GroupOperator defaultOperator) where T : IQueryVisitorContext
    {
        context.DefaultOperator = defaultOperator;

        return context;
    }

    public static T SetValue<T>(this T context, string key, object value) where T : IQueryVisitorContext
    {
        context.Data[key] = value;

        return context;
    }

    public static T GetValue<T>(this IQueryVisitorContext context, string key)
    {
        if (context.Data.TryGetValue(key, out var value) && value is T typedValue)
            return typedValue;

        return default;
    }

    public static ICollection<T> GetCollection<T>(this IQueryVisitorContext context, string key)
    {
        if (context.Data.TryGetValue(key, out var value))
        {
            if (value is ICollection<T> typedValue)
                return typedValue;

            throw new InvalidOperationException($"Context data already contains a value for \"{key}\" that is not a collection of the requested type.");
        }

        var collection = new List<T>();
        SetValue(context, key, collection);

        return collection;
    }

    public static DateTime? GetDate(this IQueryVisitorContext context, string key)
    {
        if (context.Data.TryGetValue(key, out var value) && value is DateTime date)
            return date;

        return null;
    }

    public static string GetString(this IQueryVisitorContext context, string key)
    {
        if (context.Data.TryGetValue(key, out var value) && value is string str)
            return str;

        return null;
    }

    public static bool GetBoolean(this IQueryVisitorContext context, string key, bool defaultValue = false)
    {
        if (context.Data.TryGetValue(key, out var value) && value is bool b)
            return b;

        return defaultValue;
    }

    private const string AlternateInvertedCriteriaKey = "@AlternateInvertedCriteria";
    public static T SetAlternateInvertedCriteria<T>(this T context, IQueryNode criteria) where T : IQueryVisitorContext
    {
        context.Data[AlternateInvertedCriteriaKey] = criteria;

        return context;
    }

    public static IQueryNode GetAlternateInvertedCriteria(this IQueryVisitorContext context)
    {
        return context.GetValue<IQueryNode>(AlternateInvertedCriteriaKey);
    }
}

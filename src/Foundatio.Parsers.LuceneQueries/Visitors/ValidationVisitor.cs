using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Extensions;
using System.Collections.Generic;
using System.Linq;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

public class ValidationVisitor : ChainableQueryVisitor {
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
        var validationInfo = context.GetValidationResult();

        if (node.HasParens)
            validationInfo.CurrentNodeDepth++;

        if (!String.IsNullOrEmpty(node.Field))
            AddField(validationInfo, node, context);

        AddOperation(validationInfo, node.GetOperationType(), node.Field);
        await base.VisitAsync(node, context).ConfigureAwait(false);

        if (node.HasParens)
            validationInfo.CurrentNodeDepth--;
    }

    public override void Visit(TermNode node, IQueryVisitorContext context) {
        var validationInfo = context.GetValidationResult();
        AddField(validationInfo, node, context);
        AddOperation(validationInfo, node.GetOperationType(), node.Field);

        var validationOptions = context.GetValidationOptions();
        if (validationOptions != null && !validationOptions.AllowLeadingWildcards && node.Term != null && (node.Term.StartsWith("*") || node.Term.StartsWith("?")))
            context.AddValidationError("Terms must not start with a wildcard: " + node.Term);
    }

    public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
        var validationInfo = context.GetValidationResult();
        AddField(validationInfo, node, context);
        AddOperation(validationInfo, node.GetOperationType(), node.Field);

        var validationOptions = context.GetValidationOptions();
        if (validationOptions != null && !validationOptions.AllowLeadingWildcards && node.Min != null && (node.Min.StartsWith("*") || node.Min.StartsWith("?")))
            context.AddValidationError("Terms must not start with a wildcard: " + node.Min);

        if (validationOptions != null && !validationOptions.AllowLeadingWildcards && node.Max != null && (node.Max.StartsWith("*") || node.Max.StartsWith("?")))
            context.AddValidationError("Terms must not start with a wildcard: " + node.Max);
    }

    public override void Visit(ExistsNode node, IQueryVisitorContext context) {
        var validationInfo = context.GetValidationResult();
        AddField(validationInfo, node, context);
        AddOperation(validationInfo, "exists", node.Field);
    }

    public override void Visit(MissingNode node, IQueryVisitorContext context) {
        var validationInfo = context.GetValidationResult();
        AddField(validationInfo, node, context);
        AddOperation(validationInfo, "missing", node.Field);
    }

    private void AddField(QueryValidationResult validationInfo, IFieldQueryNode node, IQueryVisitorContext context) {
        if (validationInfo == null)
            return;

        if (!String.IsNullOrEmpty(node.Field)) {
            if (node.Field.StartsWith("@"))
                return;

            validationInfo.ReferencedFields.Add(node.Field);
        } else {
            var fields = node.GetDefaultFields(context.DefaultFields);
            if (fields == null || fields.Length == 0)
                validationInfo.ReferencedFields.Add("");
            else
                foreach (string defaultField in fields)
                    validationInfo.ReferencedFields.Add(defaultField);
        }
    }

    private void AddOperation(QueryValidationResult info, string operation, string field) {
        if (String.IsNullOrEmpty(operation))
            return;

        info.AddOperation(operation, field);
    }

    public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
        await node.AcceptAsync(this, context).ConfigureAwait(false);
        var validationInfo = context.GetValidationResult();
        validationInfo.QueryType = context.QueryType;
        ApplyQueryRestrictions(context);

        return node;
    }

    internal void ApplyQueryRestrictions(IQueryVisitorContext context) {
        var options = context.GetValidationOptions();
        var info = context.GetValidationResult();

        if (options.AllowedFields.Count > 0) {
            var nonAllowedFields = info.ReferencedFields.Where(f => !String.IsNullOrEmpty(f) && !options.AllowedFields.Contains(f)).ToArray();
            if (nonAllowedFields.Length > 0)
                context.AddValidationError($"Query uses field(s) ({String.Join(",", nonAllowedFields)}) that are not allowed to be used.");
        }

        if (options.AllowedOperations.Count > 0) {
            var nonAllowedOperations = info.Operations.Where(f => !options.AllowedOperations.Contains(f.Key)).ToArray();
            if (nonAllowedOperations.Length > 0)
                context.AddValidationError($"Query uses aggregation operations ({String.Join(",", nonAllowedOperations)}) that are not allowed to be used.");
        }

        if (!options.AllowUnresolvedFields && info.UnresolvedFields.Count > 0)
            context.AddValidationError($"Query uses field(s) ({String.Join(",", info.UnresolvedFields)}) that can't be resolved.");

        if (options.AllowedMaxNodeDepth > 0 && info.MaxNodeDepth > options.AllowedMaxNodeDepth)
            context.AddValidationError($"Query has a node depth {info.MaxNodeDepth} greater than the allowed maximum {options.AllowedMaxNodeDepth}.");

        if (options.ShouldThrow && !info.IsValid)
            throw new QueryValidationException($"Invalid query: {info.Message}", info);
    }

    public static async Task<QueryValidationResult> RunAsync(IQueryNode node, IQueryVisitorContextWithValidation context = null) {
        if (context == null)
            context = new QueryVisitorContext();

        var visitor = new ChainedQueryVisitor();
        if (context.QueryType == QueryTypes.Aggregation)
            visitor.AddVisitor(new AssignOperationTypeVisitor());
        if (context.QueryType == QueryTypes.Sort)
            visitor.AddVisitor(new TermToFieldVisitor());
        visitor.AddVisitor(new ValidationVisitor());

        await visitor.AcceptAsync(node, context);
        return context.GetValidationResult();
    }

    public static QueryValidationResult Run(IQueryNode node, IQueryVisitorContextWithValidation context = null) {
        return RunAsync(node, context).GetAwaiter().GetResult();
    }

    public static async Task<QueryValidationResult> RunAsync(IQueryNode node, QueryValidationOptions options, IQueryVisitorContextWithValidation context = null) {
        if (context == null)
            context = new QueryVisitorContext();

        if (options != null)
            context.SetValidationOptions(options);

        await new ValidationVisitor().AcceptAsync(node, context);
        var validationInfo = context.GetValidationResult();
        return validationInfo;
    }

    public static Task<QueryValidationResult> RunAsync(IQueryNode node, IEnumerable<string> allowedFields, IQueryVisitorContextWithValidation context = null) {
        var options = new QueryValidationOptions();
        foreach (var field in allowedFields)
            options.AllowedFields.Add(field);
        return RunAsync(node, options, context);
    }

    public static QueryValidationResult Run(IQueryNode node, IEnumerable<string> allowedFields, IQueryVisitorContextWithValidation context = null) {
        return RunAsync(node, allowedFields, context).GetAwaiter().GetResult();
    }
}

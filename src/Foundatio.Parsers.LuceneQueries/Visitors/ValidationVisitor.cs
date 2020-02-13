using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Extensions;
using System.Collections.Generic;
using System.Linq;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class ValidationVisitor : ChainableQueryVisitor {
        public bool ShouldThrow { get; set; }

        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();

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
            var validationInfo = context.GetValidationInfo();
            AddField(validationInfo, node, context);
            AddOperation(validationInfo, node.GetOperationType(), node.Field);
            
            // aggregations must have a field
            if (context.QueryType == QueryType.Aggregation && String.IsNullOrEmpty(node.Field))
                validationInfo.IsValid = false;
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            AddField(validationInfo, node, context);
            AddOperation(validationInfo, node.GetOperationType(), node.Field);
            
            // aggregations must have a field
            if (context.QueryType == QueryType.Aggregation && String.IsNullOrEmpty(node.Field))
                validationInfo.IsValid = false;
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            AddField(validationInfo, node, context);
            AddOperation(validationInfo, "exists", node.Field);
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            AddField(validationInfo, node, context);
            AddOperation(validationInfo, "missing", node.Field);
        }

        private void AddField(QueryValidationInfo validationInfo, IFieldQueryNode node, IQueryVisitorContext context) {
            if (!String.IsNullOrEmpty(node.Field)) {
                if (!node.Field.StartsWith("@"))
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

        private void AddOperation(QueryValidationInfo info, string operation, string field) {
            if (String.IsNullOrEmpty(operation))
                return;

            info.Operations.AddOrUpdate(operation,
                op => new HashSet<string>(StringComparer.OrdinalIgnoreCase) { field },
                (op, collection) => {
                    collection.Add(field);
                    return collection;
                }
            );
        }

        public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            await node.AcceptAsync(this, context).ConfigureAwait(false);
            var validationInfo = context.GetValidationInfo();
            validationInfo.QueryType = context.QueryType;
            var validator = context.GetValidator();
            if (validator != null && ShouldThrow && !await validator(validationInfo))
                throw new QueryValidationException("Invalid query.", validationInfo);
            
            return node;
        }

        public static async Task<QueryValidationInfo> RunAsync(IQueryNode node, IQueryVisitorContextWithValidator context = null) {
            if (context == null)
                context = new QueryVisitorContextWithValidator();
            
            var visitor = new ChainedQueryVisitor();
            if (context.QueryType == QueryType.Aggregation)
                visitor.AddVisitor(new AssignOperationTypeVisitor());
            if (context.QueryType == QueryType.Sort)
                visitor.AddVisitor(new TermToFieldVisitor());
            visitor.AddVisitor(new ValidationVisitor());

            await visitor.AcceptAsync(node, context);
            return context.GetValidationInfo();
        }

        public static QueryValidationInfo Run(IQueryNode node, IQueryVisitorContextWithValidator context = null) {
            return RunAsync(node, context).GetAwaiter().GetResult();
        }

        public static async Task<bool> RunAsync(IQueryNode node, Func<QueryValidationInfo, Task<bool>> validator, IQueryVisitorContextWithValidator context = null) {
            if (context == null)
                context = new QueryVisitorContextWithValidator();

            await new ValidationVisitor().AcceptAsync(node, context);
            var validationInfo = context.GetValidationInfo();

            return await validator(validationInfo);
        }

        public static bool Run(IQueryNode node, Func<QueryValidationInfo, Task<bool>> validator, IQueryVisitorContextWithValidator context = null) {
            return RunAsync(node, validator, context).GetAwaiter().GetResult();
        }

        public static Task<bool> RunAsync(IQueryNode node, IEnumerable<string> allowedFields, IQueryVisitorContextWithValidator context = null) {
            var fieldSet = new HashSet<string>(allowedFields, StringComparer.OrdinalIgnoreCase);
            return RunAsync(node, info => Task.FromResult(info.ReferencedFields.Any(f => !fieldSet.Contains(f))), context);
        }

        public static bool Run(IQueryNode node, IEnumerable<string> allowedFields, IQueryVisitorContextWithValidator context = null) {
            return RunAsync(node, allowedFields, context).GetAwaiter().GetResult();
        }
    }

    public class QueryValidationInfo {
        public string QueryType { get; set; }
        public bool IsValid { get; set; } = true;
        public ICollection<string> ReferencedFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public int MaxNodeDepth { get; set; } = 1;
        public ConcurrentDictionary<string, ICollection<string>> Operations { get; } = new ConcurrentDictionary<string, ICollection<string>>(StringComparer.OrdinalIgnoreCase);

        private int _currentNodeDepth = 1;
        internal int CurrentNodeDepth {
            get => _currentNodeDepth;
            set {
                _currentNodeDepth = value;
                if (_currentNodeDepth > MaxNodeDepth)
                    MaxNodeDepth = _currentNodeDepth;
            }
        }
    }

    public static class QueryType {
        public const string Query = "query";
        public const string Aggregation = "aggregation";
        public const string Sort = "sort";
        public const string Unknown = "unknown";
    }

    public class QueryValidationException : Exception {
        public QueryValidationException(string message, QueryValidationInfo validationInfo = null,
            Exception inner = null) : base(message, inner) {
            ValidationInfo = validationInfo;
        }
        
        public QueryValidationInfo ValidationInfo { get; }
    }
}
using System;
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
            if (validationInfo.QueryType == null)
                validationInfo.QueryType = node.GetQueryType();

            if (!String.IsNullOrEmpty(node.Field)) {
                validationInfo.ReferencedFields.Add(node.GetFullName());
                validationInfo.OperationCount++;

                if (node.HasParens)
                    validationInfo.CurrentNodeDepth++;
            }

            string operationType = node.GetOperationType();
            if (!String.IsNullOrEmpty(operationType))
                validationInfo.OperationTypes.Add(operationType);

            await base.VisitAsync(node, context).ConfigureAwait(false);

            if (!String.IsNullOrEmpty(node.Field) && node.HasParens)
                validationInfo.CurrentNodeDepth--;
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            if (!String.IsNullOrEmpty(node.Field)) {
                validationInfo.ReferencedFields.Add(node.GetFullName());
            } else {
                var nameParts = node.GetNameParts();
                if (nameParts.Length == 0)
                    validationInfo.ReferencedFields.Add("_all");
            }

            string operationType = node.GetOperationType();
            if (!String.IsNullOrEmpty(operationType))
                validationInfo.OperationTypes.Add(operationType);

            validationInfo.OperationCount++;
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            if (!String.IsNullOrEmpty(node.Field)) {
                validationInfo.ReferencedFields.Add(node.GetFullName());
            } else {
                var nameParts = node.GetNameParts();
                if (nameParts.Length == 0)
                    validationInfo.ReferencedFields.Add("_all");
            }

            string operationType = node.GetOperationType();
            if (!String.IsNullOrEmpty(operationType))
                validationInfo.OperationTypes.Add(operationType);

            validationInfo.OperationCount++;
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            if (!String.IsNullOrEmpty(node.Field))
                validationInfo.ReferencedFields.Add(node.GetFullName());

            validationInfo.OperationTypes.Add("exists");
            validationInfo.OperationCount++;
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            if (!String.IsNullOrEmpty(node.Field))
                validationInfo.ReferencedFields.Add(node.GetFullName());

            validationInfo.OperationTypes.Add("missing");
            validationInfo.OperationCount++;
        }

        public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            await node.AcceptAsync(this, context).ConfigureAwait(false);
            var validationInfo = context.GetValidationInfo();
            var validator = context.GetValidator();
            if (validator != null && ShouldThrow && !await validator(validationInfo))
                throw new QueryValidationException();

            return node;
        }

        public static async Task<bool> RunAsync(GroupNode node, Func<QueryValidationInfo, Task<bool>> validator, IQueryVisitorContext context = null) {
            await new ValidationVisitor().AcceptAsync(node, context ?? new QueryVisitorContext());
            var validationInfo = context.GetValidationInfo();

            return await validator(validationInfo);
        }

        public static bool Run(GroupNode node, Func<QueryValidationInfo, Task<bool>> validator, IQueryVisitorContext context = null) {
            return RunAsync(node, validator, context).GetAwaiter().GetResult();
        }

        public static bool Run(GroupNode node, Func<QueryValidationInfo, bool> validator, IQueryVisitorContext context = null) {
            return RunAsync(node, info => Task.FromResult(validator(info)), context).GetAwaiter().GetResult();
        }

        public static bool Run(GroupNode node, IEnumerable<string> allowedFields, IQueryVisitorContext context = null) {
            var fieldSet = new HashSet<string>(allowedFields, StringComparer.OrdinalIgnoreCase);
            return Run(node, info => info.ReferencedFields.Any(f => !fieldSet.Contains(f)), context);
        }
    }

    public class QueryValidationInfo {
        public string QueryType { get; set; }
        public ICollection<string> ReferencedFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public int MaxNodeDepth { get; set; } = 1;
        public int OperationCount { get; set; }
        public ICollection<string> OperationTypes { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private int _currentNodeDepth = 1;
        internal int CurrentNodeDepth {
            get { return _currentNodeDepth; }
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

    public class QueryValidationException : ApplicationException { }
}
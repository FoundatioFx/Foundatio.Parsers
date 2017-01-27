using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Extensions;
using System.Collections.Generic;
using System.Linq;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class ValidationVisitor : ChainableQueryVisitor {
        private static readonly LuceneQueryParser _parser = new LuceneQueryParser();

        public bool ShouldThrow { get; set; }

        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            if (validationInfo.QueryType == null)
                validationInfo.QueryType = node.GetQueryType();

            string field = null;
            if (!String.IsNullOrEmpty(node.Field)) {
                field = node.GetFullName();
                validationInfo.ReferencedFields.Add(field);

                if (node.HasParens)
                    validationInfo.CurrentNodeDepth++;
            }

            AddOperation(validationInfo, node.GetOperationType(), field);
            await base.VisitAsync(node, context).ConfigureAwait(false);
            if (!String.IsNullOrEmpty(node.Field) && node.HasParens)
                validationInfo.CurrentNodeDepth--;
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            string field = null;
            if (!String.IsNullOrEmpty(node.Field)) {
                field = node.GetFullName();
                validationInfo.ReferencedFields.Add(field);
            } else {
                if (String.Equals(validationInfo.QueryType, QueryType.Aggregation))
                    validationInfo.IsValid = false;

                var nameParts = node.GetNameParts();
                if (nameParts.Length == 0)
                    validationInfo.ReferencedFields.Add("_all");
            }

            AddOperation(validationInfo, node.GetOperationType(), field);
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            string field = null;
            if (!String.IsNullOrEmpty(node.Field)) {
                field = node.GetFullName();
                validationInfo.ReferencedFields.Add(field);
            } else {
                if (String.Equals(validationInfo.QueryType, QueryType.Aggregation))
                    validationInfo.IsValid = false;

                var nameParts = node.GetNameParts();
                if (nameParts.Length == 0)
                    validationInfo.ReferencedFields.Add("_all");
            }

            AddOperation(validationInfo, node.GetOperationType(), field);
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            string field = null;
            if (!String.IsNullOrEmpty(node.Field)) {
                field = node.GetFullName();
                validationInfo.ReferencedFields.Add(field);
            }

            AddOperation(validationInfo, "exists", field);
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            string field = null;
            if (!String.IsNullOrEmpty(node.Field)) {
                field = node.GetFullName();
                validationInfo.ReferencedFields.Add(field);
            }

            AddOperation(validationInfo, "missing", field);
        }

        public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            await node.AcceptAsync(this, context).ConfigureAwait(false);
            var validationInfo = context.GetValidationInfo();
            var validator = context.GetValidator();
            if (validator != null && ShouldThrow && !await validator(validationInfo))
                throw new QueryValidationException();

            return node;
        }

        private void AddOperation(QueryValidationInfo info, string operation, string field) {
            if (String.IsNullOrEmpty(operation))
                return;

            if (String.IsNullOrEmpty(field) || String.Equals(field, "_all")) {
                info.IsValid = false;
                return;
            }

            info.Operations.AddOrUpdate(operation,
                op => new HashSet<string>(StringComparer.OrdinalIgnoreCase) { field },
                (op, collection) => {
                    collection.Add(field);
                    return collection;
                }
            );
        }

        public static async Task<QueryValidationInfo> RunAsync(IQueryNode node, IQueryVisitorContextWithValidator context = null) {
            if (context == null)
                context = new QueryVisitorContextWithValidator();

            await new ValidationVisitor().AcceptAsync(node, context);
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
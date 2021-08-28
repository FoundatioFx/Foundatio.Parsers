using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Extensions;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class ValidationVisitor : ChainableQueryVisitor {
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
                validationInfo.MarkInvalid("Aggregations must have a field");
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            var validationInfo = context.GetValidationInfo();
            AddField(validationInfo, node, context);
            AddOperation(validationInfo, node.GetOperationType(), node.Field);
            
            // aggregations must have a field
            if (context.QueryType == QueryType.Aggregation && String.IsNullOrEmpty(node.Field))
                validationInfo.MarkInvalid("Aggregations must have a field");
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
                if (node.Field.StartsWith("@"))
                    return;

                validationInfo.ReferencedFields.Add(node.Field);
                var validationOptions = context.GetValidationOptions();
                if (validationOptions == null || !validationOptions.ShouldResolveFields)
                    return;

                if (!CanResolveField(node.Field, context))
                    validationInfo.UnresolvedFields.Add(node.Field);
            } else {
                var fields = node.GetDefaultFields(context.DefaultFields);
                if (fields == null || fields.Length == 0)
                    validationInfo.ReferencedFields.Add("");
                else
                    foreach (string defaultField in fields)
                        validationInfo.ReferencedFields.Add(defaultField);
            }
        }

        protected virtual bool CanResolveField(string field, IQueryVisitorContext context) {
            var resolver = context.GetFieldResolver();
            if (resolver == null)
                throw new InvalidOperationException("Field resolver not configured when using ShouldResolveFields query validation option.");

            try {
                var resolvedField = resolver(field);
                return resolvedField != null;
            } catch {
                // TODO: This should be logged but not blow up
                return false;
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
            var options = context.GetValidationOptions();
            if (options != null) {
                await validationInfo.ApplyOptionsAsync(options);

                if (options.ShouldThrow && !validationInfo.IsValid)
                    throw new QueryValidationException($"Invalid query: {validationInfo.Message}", validationInfo);
            }
            
            return node;
        }

        public static async Task<QueryValidationInfo> RunAsync(IQueryNode node, IQueryVisitorContextWithValidation context = null) {
            if (context == null)
                context = new QueryVisitorContext();

            var visitor = new ChainedQueryVisitor();
            if (context.QueryType == QueryType.Aggregation)
                visitor.AddVisitor(new AssignOperationTypeVisitor());
            if (context.QueryType == QueryType.Sort)
                visitor.AddVisitor(new TermToFieldVisitor());
            visitor.AddVisitor(new ValidationVisitor());

            await visitor.AcceptAsync(node, context);
            return context.GetValidationInfo();
        }

        public static QueryValidationInfo Run(IQueryNode node, IQueryVisitorContextWithValidation context = null) {
            return RunAsync(node, context).GetAwaiter().GetResult();
        }

        public static async Task<bool> RunAsync(IQueryNode node, QueryValidationOptions options, IQueryVisitorContextWithValidation context = null) {
            if (context == null)
                context = new QueryVisitorContext();

            if (options != null)
                context.SetValidationOptions(options);

            await new ValidationVisitor().AcceptAsync(node, context);
            var validationInfo = context.GetValidationInfo();
            return validationInfo.IsValid;
        }

        public static bool Run(IQueryNode node, Func<QueryValidationInfo, Task<bool>> validator, IQueryVisitorContextWithValidation context = null) {
            var options = new QueryValidationOptions {
                CustomValidationCallback = async i => (await validator(i), null)
            };
            return RunAsync(node, options, context).GetAwaiter().GetResult();
        }

        public static bool Run(IQueryNode node, Func<QueryValidationInfo, Task<(bool, string)>> validator, IQueryVisitorContextWithValidation context = null) {
            var options = new QueryValidationOptions {
                CustomValidationCallback = validator
            };
            return RunAsync(node, options, context).GetAwaiter().GetResult();
        }

        public static Task<bool> RunAsync(IQueryNode node, IEnumerable<string> allowedFields, IQueryVisitorContextWithValidation context = null) {
            var options = new QueryValidationOptions();
            foreach (var field in allowedFields)
                options.AllowedFields.Add(field);
            return RunAsync(node, options, context);
        }

        public static bool Run(IQueryNode node, IEnumerable<string> allowedFields, IQueryVisitorContextWithValidation context = null) {
            return RunAsync(node, allowedFields, context).GetAwaiter().GetResult();
        }
    }

    public class QueryValidationOptions {
        private bool _allowUnresolvedFields;

        public bool ShouldThrow { get; set; }
        public ICollection<string> AllowedFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public bool ShouldResolveFields { get; set; }
        public bool AllowUnresolvedFields {
            get => _allowUnresolvedFields;
            set {
                _allowUnresolvedFields = value;
                if (!_allowUnresolvedFields)
                    ShouldResolveFields = true;
            }
        }
        public ICollection<string> AllowedOperations { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public int AllowedMaxNodeDepth { get; set; }
        public Func<QueryValidationInfo, Task<(bool, string)>> CustomValidationCallback { get; set; }
    }

    [DebuggerDisplay("IsValid: {IsValid} Message: {Message} Type: {QueryType}")]
    public class QueryValidationInfo {
        public string QueryType { get; set; }
        public bool IsValid { get; private set; } = true;
        public string Message { get; set;}
        public ICollection<string> ReferencedFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public ICollection<string> UnresolvedFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
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

        public void MarkInvalid(string message) {
            IsValid = false;
            Message = message;
        }

        internal async Task ApplyOptionsAsync(QueryValidationOptions options) {
            if (options == null)
                return;

            if (options.AllowedFields.Count > 0) {
                var nonAllowedFields = ReferencedFields.Where(f => !String.IsNullOrEmpty(f) && !options.AllowedFields.Contains(f)).ToArray();
                if (nonAllowedFields.Length > 0) {
                    MarkInvalid($"Query uses fields ({String.Join(",", nonAllowedFields)}) that are not allowed to be used.");
                    return;
                }
            }

            if (options.AllowedOperations.Count > 0) {
                var nonAllowedOperations = Operations.Where(f => !options.AllowedOperations.Contains(f.Key)).ToArray();
                if (nonAllowedOperations.Length > 0) {
                    MarkInvalid($"Query uses aggregation operations ({String.Join(",", nonAllowedOperations)}) that are not allowed to be used.");
                    return;
                }
            }

            if (!options.AllowUnresolvedFields && UnresolvedFields.Count > 0) {
                MarkInvalid($"Query uses fields ({String.Join(",", UnresolvedFields)}) that can't be resolved.");
                return;
            }

            if (options.CustomValidationCallback != null) {
                var (isValid, message) = await options.CustomValidationCallback(this).ConfigureAwait(false);
                if (!isValid) {
                    MarkInvalid($"Query is not valid: {message}");
                    return;
                }
            }

            if (options.AllowedMaxNodeDepth > 0 && MaxNodeDepth > options.AllowedMaxNodeDepth) {
                MarkInvalid($"Query has a node depth {MaxNodeDepth} greater than the allowed maximum {options.AllowedMaxNodeDepth}.");
                return;
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
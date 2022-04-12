using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Foundatio.Parsers.LuceneQueries;

public class QueryValidationOptions {
    public bool ShouldThrow { get; set; }
    public ICollection<string> AllowedFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public bool AllowLeadingWildcards { get; set; } = true;
    public bool AllowUnresolvedFields { get; set; } = true;
    public bool AllowUnresolvedIncludes { get; set; } = false;
    public ICollection<string> AllowedOperations { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public int AllowedMaxNodeDepth { get; set; }
}

[DebuggerDisplay("IsValid: {IsValid} Message: {Message} Type: {QueryType}")]
public class QueryValidationResult {
    private ConcurrentDictionary<string, ICollection<string>> _operations = new(StringComparer.OrdinalIgnoreCase);

    public string QueryType { get; set; }
    public bool IsValid => ValidationErrors.Count == 0;
    public ICollection<QueryValidationError> ValidationErrors { get; } = new List<QueryValidationError>();
    public string Message {
        get {
            if (ValidationErrors.Count == 0)
                return String.Empty;

            if (ValidationErrors.Count > 1)
                return String.Join("\r\n", ValidationErrors.Select(e => e.ToString()));

            return ValidationErrors.Single().Message;
        }
    }
    public ICollection<string> ReferencedFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public ICollection<string> ReferencedIncludes { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public ICollection<string> UnresolvedFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public ICollection<string> UnresolvedIncludes { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public int MaxNodeDepth { get; set; } = 1;
    public IDictionary<string, ICollection<string>> Operations => _operations;

    public static implicit operator bool(QueryValidationResult info) => info.IsValid;

    private int _currentNodeDepth = 1;
    internal int CurrentNodeDepth {
        get => _currentNodeDepth;
        set {
            _currentNodeDepth = value;
            if (_currentNodeDepth > MaxNodeDepth)
                MaxNodeDepth = _currentNodeDepth;
        }
    }

    internal void AddOperation(string operation, string field) {
        _operations.AddOrUpdate(operation,
            op => new HashSet<string>(StringComparer.OrdinalIgnoreCase) { field },
            (op, collection) => {
                collection.Add(field);
                return collection;
            }
        );
    }
}

public class QueryValidationError {
    public QueryValidationError(string message, int index = -1) {
        Message = message;
        Index = index;
    }

    /// <summary>
    /// The validation error message.
    /// </summary>
    public string Message { get; }

    /// <summary>
    /// Index where the validation error occurs in the query string.
    /// </summary>
    public int Index { get; } = -1;

    public override string ToString() {
        if (Index > 0)
            return $"[{Index}] {Message}";

        return Message;
    }
}

public class QueryValidationException : Exception {
    public QueryValidationException(string message, QueryValidationResult result = null,
        Exception inner = null) : base(message, inner) {
        Result = result;
    }

    public QueryValidationResult Result { get; }
    public ICollection<QueryValidationError> Errors => Result.ValidationErrors;
}

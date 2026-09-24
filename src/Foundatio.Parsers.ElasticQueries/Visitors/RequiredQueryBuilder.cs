using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

/// <summary>Builds OR groups containing + clauses without making their optional clauses mandatory.</summary>
internal static class RequiredQueryBuilder
{
    /// <summary>Captures an OR group's operands and required status before child queries are generated.</summary>
    public static async Task<List<(IFieldQueryNode Node, bool Required)>?> GetClausesAsync(GroupNode node, IElasticQueryVisitorContext context)
    {
        if (node.GetOperator(context) is not GroupOperator.Or)
            return null;

        // The parser uses binary groups even for a OR b OR c without parentheses.
        // Unmodified groups with the same OR operator belong to one list of clauses;
        // parentheses, field scopes, modifiers and custom queries keep their own boundaries.
        bool isImplicit = await IsImplicitGroupAsync(node).AnyContext();
        if (isImplicit && node.Parent is GroupNode parent && parent.GetOperator(context) is GroupOperator.Or)
            return null;

        var clauses = new List<(IFieldQueryNode Node, bool Required)>();
        var pending = new Stack<IQueryNode>();
        PushChildren(node);
        bool hasRequired = false;
        while (pending.TryPop(out var child))
        {
            if (child is GroupNode group && group.GetOperator(context) is GroupOperator.Or)
            {
                bool flatten = await IsImplicitGroupAsync(group).AnyContext();
                if (flatten)
                {
                    PushChildren(group);

                    continue;
                }
            }

            if (child is IFieldQueryNode fieldNode)
            {
                bool required = await HasRequiredClauseAsync(fieldNode).AnyContext();
                clauses.Add((fieldNode, required));
                hasRequired |= required;
            }
        }

        return hasRequired ? clauses : null;

        void PushChildren(GroupNode group)
        {
            // Push right first so clauses retain their source order without materializing Children.
            if (group.Right is { } right)
                pending.Push(right);
            if (group.Left is { } left)
                pending.Push(left);
        }
    }

    /// <summary>Combines generated operands while preserving required conditions and nested correlation.</summary>
    public static async Task<Query> BuildAsync(List<(IFieldQueryNode Node, bool Required)> clauses, Query? initial, IElasticQueryVisitorContext context)
    {
        var root = new ClauseSet(context.UseScoring);
        if (initial is not null)
            root.Add(initial);

        var nestedClauses = new Dictionary<string, ClauseSet>(StringComparer.Ordinal);
        var excludedNested = new Dictionary<string, List<Query>>(StringComparer.Ordinal);
        foreach (var (child, required) in clauses)
        {
            var query = await child.GetQueryAsync(() => child.GetDefaultQueryAsync(context)).AnyContext();
            if (query is null)
            {
                if (required)
                    context.AddValidationError($"A required clause did not produce a query: {child}");

                continue;
            }

            bool explicitNestedGroup = child is GroupNode group && group.GetNestedPath() is not null;
            if (query.Nested is not { Path: not null } nested || explicitNestedGroup)
            {
                root.Add(query, required, child.IsExcluded());

                continue;
            }

            string path = nested.Path.ToString();
            Query inner = nested.Query;
            if (child.GetNestedFilter() is { } filter)
                inner = new BoolQuery { Must = [inner], Filter = [filter] };

            if (child.IsExcluded())
            {
                if (!excludedNested.TryGetValue(path, out var excluded))
                    excludedNested[path] = excluded = [];
                excluded.Add(inner);
            }
            else
            {
                GetClauseSet(path).Add(inner, required);
            }
        }

        var originalPaths = nestedClauses.Keys.Union(excludedNested.Keys, StringComparer.Ordinal).ToHashSet(StringComparer.Ordinal);
        var ancestorCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        var requiredPaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (string path in originalPaths)
        {
            var chain = NestedPathResolver.GetNestedPathChain(path, context);
            if (nestedClauses.TryGetValue(path, out var clausesAtPath) && clausesAtPath.HasRequired)
                requiredPaths.UnionWith(chain);

            foreach (string ancestor in chain.Take(chain.Count - 1))
                ancestorCounts[ancestor] = ancestorCounts.GetValueOrDefault(ancestor) + 1;
        }

        var paths = new HashSet<string>(originalPaths, StringComparer.Ordinal);
        foreach (var (ancestor, count) in ancestorCounts)
            if (count > 1 || originalPaths.Contains(ancestor))
                paths.Add(ancestor);

        var sortedPaths = paths.OrderByDescending(path => path.Length).ToArray();
        foreach (string path in sortedPaths)
        {
            string? parentPath = sortedPaths.FirstOrDefault(candidate => candidate.Length < path.Length
                && path.StartsWith(candidate + ".", StringComparison.Ordinal));
            var target = parentPath is null ? root : GetClauseSet(parentPath);
            if (nestedClauses.TryGetValue(path, out var pathClauses) && pathClauses.Build() is { } inner)
                target.Add(new NestedQuery(path, inner), pathClauses.HasRequired);

            if (excludedNested.TryGetValue(path, out var excluded))
            {
                // Correlate exclusions only within a required nested ancestor. Otherwise
                // keep them at the root: a clean sibling must not hide a prohibited match.
                string? requiredParent = sortedPaths.FirstOrDefault(candidate => requiredPaths.Contains(candidate)
                    && candidate.Length < path.Length && path.StartsWith(candidate + ".", StringComparison.Ordinal));
                var exclusionTarget = requiredParent is null ? root : GetClauseSet(requiredParent);
                foreach (var query in excluded)
                    exclusionTarget.Add(new NestedQuery(path, query), excluded: true);
            }
        }

        return root.Build() ?? new MatchNoneQuery();

        ClauseSet GetClauseSet(string path)
        {
            if (!nestedClauses.TryGetValue(path, out var set))
                nestedClauses[path] = set = new ClauseSet(context.UseScoring);

            return set;
        }
    }

    /// <summary>Identifies parser-created grouping that adds no user-defined scope or custom query.</summary>
    private static async Task<bool> IsImplicitGroupAsync(GroupNode node)
    {
        if (node.HasParens || !String.IsNullOrEmpty(node.Field) || node.Prefix is not null
            || node.IsNegated is true || node.Boost is not null || node.Proximity is not null)
            return false;

        var query = await node.GetQueryAsync().AnyContext();
        return query is null;
    }

    /// <summary>Preserves required conditions across implicit operator groups without crossing explicit scopes.</summary>
    private static async Task<bool> HasRequiredClauseAsync(IFieldQueryNode node)
    {
        if (node.IsExcluded())
            return false;
        if (node.IsRequired())
            return true;
        if (node is not GroupNode group)
            return false;

        bool isImplicit = await IsImplicitGroupAsync(group).AnyContext();
        if (!isImplicit)
            return false;

        // Keep an implicit AND subtree intact, but require it when it contains +.
        bool leftRequired = group.Left is IFieldQueryNode left && await HasRequiredClauseAsync(left).AnyContext();
        if (leftRequired)
            return true;

        return group.Right is IFieldQueryNode right && await HasRequiredClauseAsync(right).AnyContext();
    }

    private sealed class ClauseSet(bool useScoring)
    {
        private List<Query>? _required;
        private List<Query>? _optional;
        private List<Query>? _excluded;

        public bool HasRequired => _required is { Count: > 0 };

        public void Add(Query query, bool required = false, bool excluded = false)
        {
            if (excluded)
                (_excluded ??= []).Add(query);
            else if (required)
                (_required ??= []).Add(query);
            else
                (_optional ??= []).Add(query);
        }

        public Query? Build()
        {
            if (_required is null && _optional is null && _excluded is null)
                return null;

            return new BoolQuery
            {
                Must = useScoring && HasRequired ? _required : null,
                Filter = !useScoring && HasRequired ? _required : null,
                Should = _optional is { Count: > 0 } ? _optional : null,
                MustNot = _excluded is { Count: > 0 } ? _excluded : null,
                MinimumShouldMatch = _optional is { Count: > 0 } ? (HasRequired ? 0 : 1) : null
            };
        }
    }
}

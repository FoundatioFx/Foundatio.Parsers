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
    public static async Task<List<IFieldQueryNode>?> GetClausesAsync(GroupNode node, IElasticQueryVisitorContext context)
    {
        if (node.GetOperator(context) is not GroupOperator.Or)
            return null;

        // The parser uses binary groups even for a OR b OR c without parentheses.
        // Unmodified groups with the same OR operator belong to one list of clauses;
        // parentheses, field scopes, modifiers and custom queries keep their own boundaries.
        if (IsImplicitGroup(node) && node.Parent is GroupNode parent && parent.GetOperator(context) is GroupOperator.Or
            && await node.GetQueryAsync().AnyContext() is null)
        {
            return null;
        }

        var clauses = new List<IFieldQueryNode>();
        var pending = new Stack<IQueryNode>();
        PushChildren(node);
        bool hasRequired = false;
        while (pending.TryPop(out var child))
        {
            if (child is GroupNode group && IsImplicitGroup(group) && group.GetOperator(context) is GroupOperator.Or
                && await group.GetQueryAsync().AnyContext() is null)
            {
                PushChildren(group);
            }
            else if (child is IFieldQueryNode fieldNode)
            {
                clauses.Add(fieldNode);
                hasRequired |= fieldNode.IsRequired() && !fieldNode.IsExcluded();
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

    private static bool IsImplicitGroup(GroupNode node) => !node.HasParens && String.IsNullOrEmpty(node.Field)
        && node.Prefix is null && node.IsNegated is not true && node.Boost is null && node.Proximity is null;

    public static async Task<Query> BuildAsync(List<IFieldQueryNode> clauses, Query? initial, IElasticQueryVisitorContext context)
    {
        var root = new ClauseSet(context.UseScoring);
        if (initial is not null)
            root.Add(initial);

        var nestedClauses = new Dictionary<string, ClauseSet>(StringComparer.Ordinal);
        var excludedNested = new Dictionary<string, List<Query>>(StringComparer.Ordinal);
        foreach (var child in clauses)
        {
            var query = await child.GetQueryAsync(() => child.GetDefaultQueryAsync(context)).AnyContext();
            if (query is null)
            {
                if (child.IsRequired() && !child.IsExcluded())
                    context.AddValidationError($"A required clause did not produce a query: {child}");

                continue;
            }

            bool explicitNestedGroup = child is GroupNode group && group.GetNestedPath() is not null;
            if (query.Nested is not { Path: not null } nested || explicitNestedGroup)
            {
                root.Add(query, child.IsRequired(), child.IsExcluded());

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
                GetClauseSet(path).Add(inner, child.IsRequired());
            }
        }

        var originalPaths = nestedClauses.Keys.Union(excludedNested.Keys, StringComparer.Ordinal).ToHashSet(StringComparer.Ordinal);
        var ancestorCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (string path in originalPaths)
        {
            var chain = NestedPathResolver.GetNestedPathChain(path, context);
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

            // A prohibited nested clause is an anti-existence test, not a negated inner term.
            if (excludedNested.TryGetValue(path, out var excluded))
                foreach (var query in excluded)
                    target.Add(new NestedQuery(path, query), excluded: true);
        }

        return root.Build() ?? new MatchNoneQuery();

        ClauseSet GetClauseSet(string path)
        {
            if (!nestedClauses.TryGetValue(path, out var set))
                nestedClauses[path] = set = new ClauseSet(context.UseScoring);
            return set;
        }
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

# Custom Visitors

Create custom visitors to implement specialized query transformations, validations, or data extraction.

## Creating a Basic Visitor

### Non-Mutating Visitor

Extend `QueryNodeVisitorBase` for read-only traversal:

```csharp
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

public class FieldCollectorVisitor : QueryNodeVisitorBase
{
    public HashSet<string> Fields { get; } = new();

    public override Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        if (!string.IsNullOrEmpty(node.Field))
            Fields.Add(node.Field);
        
        return Task.CompletedTask;
    }

    public override Task VisitAsync(TermRangeNode node, IQueryVisitorContext context)
    {
        if (!string.IsNullOrEmpty(node.Field))
            Fields.Add(node.Field);
        
        return Task.CompletedTask;
    }

    public override Task VisitAsync(ExistsNode node, IQueryVisitorContext context)
    {
        if (!string.IsNullOrEmpty(node.Field))
            Fields.Add(node.Field);
        
        return Task.CompletedTask;
    }
}
```

Usage:

```csharp
var parser = new LuceneQueryParser();
var result = await parser.ParseAsync("status:active AND created:[2024-01-01 TO 2024-12-31]");

var visitor = new FieldCollectorVisitor();
await visitor.AcceptAsync(result, new QueryVisitorContext());

foreach (var field in visitor.Fields)
{
    Console.WriteLine($"Field: {field}");
}
// Output: status, created
```

### Mutating Visitor

Extend `MutatingQueryNodeVisitorBase` to modify nodes:

```csharp
public class FieldPrefixVisitor : MutatingQueryNodeVisitorBase
{
    private readonly string _prefix;

    public FieldPrefixVisitor(string prefix)
    {
        _prefix = prefix;
    }

    public override Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        if (!string.IsNullOrEmpty(node.Field) && !node.Field.StartsWith(_prefix))
        {
            node.Field = _prefix + node.Field;
        }
        
        return Task.CompletedTask;
    }

    public override Task VisitAsync(TermRangeNode node, IQueryVisitorContext context)
    {
        if (!string.IsNullOrEmpty(node.Field) && !node.Field.StartsWith(_prefix))
        {
            node.Field = _prefix + node.Field;
        }
        
        return Task.CompletedTask;
    }
}
```

Usage:

```csharp
var parser = new LuceneQueryParser();
var result = await parser.ParseAsync("status:active");

var visitor = new FieldPrefixVisitor("data.");
await visitor.AcceptAsync(result, new QueryVisitorContext());

string query = await GenerateQueryVisitor.RunAsync(result);
// Output: "data.status:active"
```

## Chainable Visitors

For use with `ElasticQueryParser` or `ChainedQueryVisitor`, implement `IChainableQueryVisitor`:

```csharp
public class CustomFilterVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        // Process group nodes
        if (node.Field == "@custom")
        {
            // Custom processing logic
            await ProcessCustomFilter(node, context);
        }

        // Continue traversal to child nodes
        await base.VisitAsync(node, context);
    }

    private Task ProcessCustomFilter(GroupNode node, IQueryVisitorContext context)
    {
        // Implementation
        return Task.CompletedTask;
    }
}
```

### Adding to ElasticQueryParser

```csharp
var parser = new ElasticQueryParser(c => c
    .AddVisitor(new CustomFilterVisitor(), priority: 100));

var query = await parser.BuildQueryAsync("@custom:(filter)");
```

## Visitor with Result

Return a value from traversal:

```csharp
public class QueryComplexityVisitor : QueryNodeVisitorWithResultBase<int>
{
    private int _complexity = 0;

    public override Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        _complexity += 1;
        return Task.CompletedTask;
    }

    public override Task VisitAsync(TermRangeNode node, IQueryVisitorContext context)
    {
        _complexity += 2; // Ranges are more complex
        return Task.CompletedTask;
    }

    public override Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        _complexity += 1;
        return base.VisitAsync(node, context);
    }

    public override Task<int> AcceptAsync(IQueryNode node, IQueryVisitorContext context)
    {
        _complexity = 0;
        return base.AcceptAsync(node, context);
    }

    protected override int GetResult()
    {
        return _complexity;
    }
}
```

Usage:

```csharp
var parser = new LuceneQueryParser();
var result = await parser.ParseAsync("(a:1 AND b:2) OR c:[1 TO 10]");

var visitor = new QueryComplexityVisitor();
int complexity = await visitor.AcceptAsync(result, new QueryVisitorContext());
Console.WriteLine($"Complexity: {complexity}");
```

## Real-World Example: Custom Filter Resolution

This example shows a visitor that resolves custom filter syntax to Elasticsearch queries:

```csharp
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

/// <summary>
/// Resolves @custom:(filter) syntax to actual queries.
/// Example: @custom:(premium) -> terms query for premium user IDs
/// </summary>
public class CustomFilterVisitor : ChainableQueryVisitor
{
    private readonly IUserService _userService;

    public CustomFilterVisitor(IUserService userService)
    {
        _userService = userService;
    }

    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        if (node.Field == "@custom" && node.Left != null)
        {
            string filterName = GetFilterName(node);
            var query = await ResolveFilter(filterName);
            
            if (query != null)
            {
                // Set the resolved query on the parent node
                node.Parent?.SetQuery(query);
            }
            
            // Clear the node's children (they've been processed)
            node.Left = null;
            node.Right = null;
        }

        await base.VisitAsync(node, context);
    }

    private string GetFilterName(GroupNode node)
    {
        if (node.Left is TermNode term)
            return term.Term;
        
        return GenerateQueryVisitor.Run(node.Left);
    }

    private async Task<QueryContainer> ResolveFilter(string filterName)
    {
        switch (filterName?.ToLowerInvariant())
        {
            case "premium":
                var premiumIds = await _userService.GetPremiumUserIdsAsync();
                return new TermsQuery { Field = "user_id", Terms = premiumIds };
            
            case "active":
                return new TermQuery { Field = "status", Value = "active" };
            
            default:
                return null;
        }
    }
}
```

Usage:

```csharp
var parser = new ElasticQueryParser(c => c
    .AddVisitor(new CustomFilterVisitor(userService), priority: 50));

// Resolves @custom:(premium) to a terms query
var query = await parser.BuildQueryAsync("@custom:(premium) AND category:electronics");
```

## Example: Date Range Expansion

Expand relative date expressions:

```csharp
public class DateRangeExpansionVisitor : ChainableMutatingQueryVisitor
{
    public override Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        if (IsDateField(node.Field) && IsRelativeDate(node.Term))
        {
            var (start, end) = ExpandRelativeDate(node.Term);
            
            // Replace term node with range node
            var rangeNode = new TermRangeNode
            {
                Field = node.Field,
                Min = start,
                Max = end,
                MinInclusive = true,
                MaxInclusive = true
            };
            
            node.ReplaceSelf(rangeNode);
        }
        
        return Task.CompletedTask;
    }

    private bool IsDateField(string field)
    {
        return field?.EndsWith("_date") == true || 
               field?.EndsWith("_at") == true ||
               field == "created" || 
               field == "updated";
    }

    private bool IsRelativeDate(string term)
    {
        return term == "today" || term == "yesterday" || 
               term == "this_week" || term == "last_week";
    }

    private (string start, string end) ExpandRelativeDate(string term)
    {
        return term switch
        {
            "today" => ("now/d", "now"),
            "yesterday" => ("now-1d/d", "now-1d/d"),
            "this_week" => ("now/w", "now"),
            "last_week" => ("now-1w/w", "now-1w/w"),
            _ => (term, term)
        };
    }
}
```

## Example: Query Logging Visitor

Log all queries for analytics:

```csharp
public class QueryLoggingVisitor : QueryNodeVisitorBase
{
    private readonly ILogger _logger;
    private readonly List<string> _terms = new();
    private readonly List<string> _fields = new();

    public QueryLoggingVisitor(ILogger logger)
    {
        _logger = logger;
    }

    public override Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        _fields.Add(node.Field ?? "_default");
        _terms.Add(node.Term);
        return Task.CompletedTask;
    }

    public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context)
    {
        _terms.Clear();
        _fields.Clear();
        
        var result = await base.AcceptAsync(node, context);
        
        _logger.LogInformation(
            "Query executed. Fields: {Fields}, Terms: {Terms}",
            string.Join(", ", _fields.Distinct()),
            string.Join(", ", _terms));
        
        return result;
    }
}
```

## Visitor Priority

When using chained visitors, priority determines execution order (lower runs first):

```csharp
var parser = new ElasticQueryParser(c => c
    // Field resolution first
    .AddVisitor(new FieldResolverQueryVisitor(resolver), priority: 10)
    
    // Then include expansion
    .AddVisitor(new IncludeVisitor(), priority: 20)
    
    // Then custom processing
    .AddVisitor(new CustomFilterVisitor(), priority: 50)
    
    // Validation last
    .AddVisitor(new ValidationVisitor(), priority: 100));
```

## Best Practices

### 1. Call Base Implementation

Always call the base implementation to continue traversal:

```csharp
public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
{
    // Your logic here
    
    // Continue to child nodes
    await base.VisitAsync(node, context);
}
```

### 2. Handle Null Fields

```csharp
public override Task VisitAsync(TermNode node, IQueryVisitorContext context)
{
    if (string.IsNullOrEmpty(node.Field))
        return Task.CompletedTask;
    
    // Process node
    return Task.CompletedTask;
}
```

### 3. Use Context for State

```csharp
public override Task VisitAsync(TermNode node, IQueryVisitorContext context)
{
    // Get state from context
    var userId = context.GetValue<string>("UserId");
    
    // Store results in context
    var fields = context.GetCollection<string>("ReferencedFields");
    fields.Add(node.Field);
    
    return Task.CompletedTask;
}
```

### 4. Make Visitors Stateless When Possible

```csharp
// Prefer stateless visitors
public class StatelessVisitor : ChainableQueryVisitor
{
    public override Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        // Use context for state, not instance fields
        var count = context.GetValue<int>("TermCount");
        context.SetValue("TermCount", count + 1);
        return Task.CompletedTask;
    }
}
```

## Next Steps

- [Visitors](./visitors) - Built-in visitors reference
- [Elasticsearch Integration](./elastic-query-parser) - Elasticsearch-specific visitors
- [Validation](./validation) - Query validation

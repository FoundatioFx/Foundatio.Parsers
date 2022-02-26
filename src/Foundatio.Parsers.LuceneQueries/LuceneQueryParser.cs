using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries;

public partial class LuceneQueryParser : IQueryParser {
    public virtual Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null) {
        var result = Parse(query);
        return Task.FromResult<IQueryNode>(result);
    }

    public IQueryNode Parse(string query, IQueryVisitorContext context) {
        return ParseAsync(query, context).GetAwaiter().GetResult();
    }
}

public interface IQueryParser {
    Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null);
}

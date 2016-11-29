using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
    public interface IQueryNode {
        IQueryNode Parent { get; set; }
        IEnumerable<IQueryNode> Children { get; }
        IDictionary<string, object> Data { get; }
        Task AcceptAsync(IQueryNodeVisitor visitor, IQueryVisitorContext context);
        string ToString();
    }

    public interface IFieldQueryNode : IQueryNode {
        bool? IsNegated { get; set; }
        string Prefix { get; set; }
        string Field { get; set; }
    }
}

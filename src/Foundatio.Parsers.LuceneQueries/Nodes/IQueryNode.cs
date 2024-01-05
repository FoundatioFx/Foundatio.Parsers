using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Nodes;

public interface IQueryNode
{
    IQueryNode Parent { get; set; }
    IEnumerable<IQueryNode> Children { get; }
    IDictionary<string, object> Data { get; }
    Task<IQueryNode> AcceptAsync(IQueryNodeVisitor visitor, IQueryVisitorContext context);
    string ToString();
    IQueryNode Clone();
}

public interface IFieldQueryNode : IQueryNode
{
    bool? IsNegated { get; set; }
    string Prefix { get; set; }
    string Field { get; set; }
    string UnescapedField { get; }
}

public interface IFieldQueryWithProximityAndBoostNode : IFieldQueryNode
{
    string Boost { get; set; }
    string UnescapedBoost { get; }
    string Proximity { get; set; }
    string UnescapedProximity { get; }
}

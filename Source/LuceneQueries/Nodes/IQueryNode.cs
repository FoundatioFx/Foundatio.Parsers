using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
    public interface IQueryNode {
        GroupNode Parent { get; set; }
        IList<IQueryNode> Children { get; }
        void Accept(IQueryNodeVisitor visitor);
        string ToString();
    }
}

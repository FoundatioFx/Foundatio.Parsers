using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Visitor;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public interface IQueryNode {
        GroupNode Parent { get; set; }
        IList<IQueryNode> Children { get; }
        void Accept(IQueryNodeVisitor visitor);
        string ToString();
    }
}

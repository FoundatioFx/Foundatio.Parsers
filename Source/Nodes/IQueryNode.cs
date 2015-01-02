using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Visitor;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public interface IQueryNode {
        IEnumerable<IQueryNode> Children { get; }
        void Accept(IQueryNodeVisitor visitor, bool visitChildren = true);
    }
}

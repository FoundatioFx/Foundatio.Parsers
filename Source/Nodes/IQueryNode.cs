using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Visitor;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public interface IQueryNode {
        IList<IQueryNode> Children { get; }
        void Accept(IQueryNodeVisitor visitor, bool visitChildren = true);
        string ToString(bool escapeTerms);
    }
}

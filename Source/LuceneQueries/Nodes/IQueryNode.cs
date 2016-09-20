using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
    public interface IQueryNode {
        IQueryNode Parent { get; set; }
        IList<IQueryNode> Children { get; }
        IDictionary<string, object> Meta { get; }
        void Accept(IQueryNodeVisitor visitor);
        string ToString();
    }

    public interface IFieldQueryNode : IQueryNode {
        bool? IsNegated { get; set; }
        string Prefix { get; set; }
        string Field { get; set; }
    }
}

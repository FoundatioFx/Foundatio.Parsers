using System.Collections.Generic;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class FieldExpressionNode : QueryNodeBase {
        public string Field { get; set; }
        public string Prefix { get; set; }

        public override IEnumerable<IQueryNode> Children {
            get { yield break; }
        }
    }
}

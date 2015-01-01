using System.Collections.Generic;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class GroupNode : QueryNodeBase {
        public IQueryNode Left { get; set; }
        public IQueryNode Right { get; set; }
        public string Operator { get; set; }
        public FieldExpressionNode Field { get; set; }

        public override IEnumerable<IQueryNode> Children {
            get {
                yield return Left;
                yield return Right;
                yield return Field;
            }
        }
    }
}
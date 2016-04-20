using System;
using System.Collections.Generic;
using System.Text;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class GroupNode : QueryNodeBase {
        public IQueryNode Left { get; set; }
        public IQueryNode Right { get; set; }
        public string Operator { get; set; }
        public bool HasParens { get; set; }
        public string Field { get; set; }
        public string Prefix { get; set; }
        public double? Boost { get; set; }

        public override String ToString() {
            var builder = new StringBuilder();

            if (!String.IsNullOrEmpty(Field)) {
                builder.Append(Prefix);
                builder.Append(Field);
                builder.Append(":");
            }

            if (HasParens)
                builder.Append("(");

            if (Left != null)
                builder.Append(Left);

            if (!String.IsNullOrEmpty(Operator))
                builder.Append(" " + Operator + " ");
            else if (Right != null)
                builder.Append(" ");

            if (Right != null)
                builder.Append(Right);

            if (HasParens)
                builder.Append(")");

            if (Boost.HasValue)
                builder.Append("^" + Boost);

            return builder.ToString();
        }

        public override IList<IQueryNode> Children {
            get {
                var children = new List<IQueryNode>();
                if (Left != null)
                    children.Add(Left);
                if (Right != null)
                    children.Add(Right);

                return children;
            }
        }
    }
}
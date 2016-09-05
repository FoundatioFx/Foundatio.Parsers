using System;
using System.Collections.Generic;
using System.Text;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class GroupNode : QueryNodeBase {
        public IQueryNode Left { get; set; }
        public IQueryNode Right { get; set; }
        public GroupOperator Operator { get; set; } = GroupOperator.Default;
        public bool HasParens { get; set; }
        public string Field { get; set; }
        public bool? IsNegated { get; set; }
        public string Prefix { get; set; }
        public string Boost { get; set; }

        public override string ToString(bool escapeTerms)
        {
            var builder = new StringBuilder();

            builder.Append(Prefix);

            if (!String.IsNullOrEmpty(Field))
            {
                builder.Append(Field);
                builder.Append(":");
            }

            if (HasParens)
                builder.Append("(");

            if (Left != null)
                builder.Append(Left.ToString(escapeTerms));

            if (Operator == GroupOperator.And)
                builder.Append(" AND ");
            else if (Operator == GroupOperator.Or)
                builder.Append(" OR ");
            else if (Right != null)
                builder.Append(" ");

            if (IsNegated.HasValue && IsNegated.Value)
                builder.Append("NOT ");

            if (Right != null)
                builder.Append(Right.ToString(escapeTerms));

            if (HasParens)
                builder.Append(")");

            if (Boost != null)
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

    public enum GroupOperator {
        Default,
        And,
        Or
    }
}
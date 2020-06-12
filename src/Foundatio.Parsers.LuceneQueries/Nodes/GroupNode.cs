using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Extensions;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
    public class GroupNode : QueryNodeBase, IFieldQueryWithProximityAndBoostNode {
        public IQueryNode Left { get; set; }
        public IQueryNode Right { get; set; }
        public GroupOperator Operator { get; set; } = GroupOperator.Default;
        public bool HasParens { get; set; }
        public string Field { get; set; }
        public bool? IsNegated { get; set; }
        public string Prefix { get; set; }
        public string Boost { get; set; }
        public string UnescapedBoost => Boost?.Unescape();
        public string Proximity { get; set; }

        public GroupNode CopyTo(GroupNode target) {
            if (Left != null)
                target.Left = Left;

            if (Right != null)
                target.Right = Right;

            target.Operator = Operator;
            target.HasParens = HasParens;

            if (Field != null)
                target.Field = Field;

            if (IsNegated.HasValue)
                target.IsNegated = IsNegated;

            if (Boost != null)
                target.Boost = Boost;

            if (Proximity != null)
                target.Proximity = Proximity;

            if (Prefix != null)
                target.Prefix = Prefix;

            foreach (var kvp in Data)
                target.Data.Add(kvp.Key, kvp.Value);

            return target;
        }

        public override string ToString() {
            var builder = new StringBuilder();

            if (IsNegated.HasValue && IsNegated.Value)
                builder.Append("NOT ");

            builder.Append(Prefix);

            if (!String.IsNullOrEmpty(Field))
                builder.Append(Field).Append(':');

            if (HasParens)
                builder.Append("(");

            if (Left != null)
                builder.Append(Left);

            if (Operator == GroupOperator.And)
                builder.Append(" AND ");
            else if (Operator == GroupOperator.Or)
                builder.Append(" OR ");
            else if (Right != null)
                builder.Append(" ");

            if (Right != null)
                builder.Append(Right);

            if (HasParens)
                builder.Append(")");

            if (Proximity != null)
                builder.Append("~" + Proximity);

            if (Boost != null)
                builder.Append("^" + Boost);

            return builder.ToString();
        }

        public override IEnumerable<IQueryNode> Children {
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
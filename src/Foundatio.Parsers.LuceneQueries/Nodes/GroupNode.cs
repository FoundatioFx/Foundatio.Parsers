using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Extensions;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
    public class GroupNode : QueryNodeBase, IFieldQueryWithProximityAndBoostNode {
        private IQueryNode _left;
        public IQueryNode Left {
            get => _left;
            set {
                _left = value;
                if (_left != null)
                    _left.Parent = this;
            }
        }

        private IQueryNode _right;
        public IQueryNode Right {
            get => _right;
            set {
                _right = value;
                if (_right != null)
                    _right.Parent = this;
            }
        }

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
            return ToString(GroupOperator.Default);
        }

        public string ToString(GroupOperator defaultOperator) {
            if (Left == null && Right == null)
                return String.Empty;

            var builder = new StringBuilder();
            var op = Operator != GroupOperator.Default ? Operator : defaultOperator;

            if (IsNegated.HasValue && IsNegated.Value)
                builder.Append("NOT ");

            builder.Append(Prefix);

            if (!String.IsNullOrEmpty(Field))
                builder.Append(Field).Append(':');

            if (HasParens)
                builder.Append("(");

            if (Left != null) {
                builder.Append(Left);

                if (op == GroupOperator.And)
                    builder.Append(" AND ");
                else if (op == GroupOperator.Or)
                    builder.Append(" OR ");
                else if (Right != null)
                    builder.Append(" ");
            }

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
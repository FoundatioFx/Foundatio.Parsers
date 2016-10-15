using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Extensions;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
    public class TermNode : QueryNodeBase, IFieldQueryNode {
        public bool? IsNegated { get; set; }
        public string Prefix { get; set; }
        public string Field { get; set; }
        public string Term { get; set; }
        public string UnescapedTerm => Term.Unescape();
        public bool IsQuotedTerm { get; set; }
        public string Boost { get; set; }
        public string Proximity { get; set; }

        public TermNode CopyTo(TermNode target) {
            if (IsNegated.HasValue)
                target.IsNegated = IsNegated;

            if (Prefix != null)
                target.Prefix = Prefix;

            if (Field != null)
                target.Field = Field;

            if (Term != null)
                target.Term = Term;

            target.IsQuotedTerm = IsQuotedTerm;

            if (Boost != null)
                target.Boost = Boost;

            if (Proximity != null)
                target.Proximity = Proximity;

            foreach (var kvp in Data)
                target.Data.Add(kvp.Key, kvp.Value);

            return target;
        }

        public override string ToString() {
            var builder = new StringBuilder();

            if (IsNegated.HasValue && IsNegated.Value)
                builder.Append("NOT ");

            builder.Append(Prefix);

            if (!String.IsNullOrEmpty(Field)) {
                builder.Append(Field);
                builder.Append(":");
            }

            builder.Append(IsQuotedTerm ? "\"" + Term + "\"" : Term);

            if (Proximity != null)
                builder.Append("~" + Proximity);

            if (Boost != null)
                builder.Append("^" + Boost);

            return builder.ToString();
        }

        public override IList<IQueryNode> Children => EmptyNodeList;
    }
}
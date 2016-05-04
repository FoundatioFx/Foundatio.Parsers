using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class TermNode : QueryNodeBase {
        public string Prefix { get; set; }
        public string Field { get; set; }
        public string Term { get; set; }
        public bool IsQuotedTerm { get; set; }
        public double? Boost { get; set; }
        public double? Proximity { get; set; }

        public TermNode CopyTo(TermNode target) {
            if (Prefix != null)
                target.Prefix = Prefix;

            if (Field != null)
                target.Field = Field;

            if (Term != null)
                target.Term = Term;

            target.IsQuotedTerm = IsQuotedTerm;

            if (Boost.HasValue)
                target.Boost = Boost;

            if (Proximity.HasValue)
                target.Proximity = Proximity;

            return target;
        }

        public override String ToString() {
            var builder = new StringBuilder();

            builder.Append(Prefix);

            if (!String.IsNullOrEmpty(Field)) {
                builder.Append(Field);
                builder.Append(":");
            }

            builder.Append(IsQuotedTerm ? "\"" + Term + "\"" : Term);

            if (Boost.HasValue)
                builder.Append("^" + Boost);

            if (Proximity.HasValue)
                builder.Append("~" + (Proximity.Value != Double.MinValue ? Proximity.ToString() : String.Empty));

            return builder.ToString();
        }

        public override IList<IQueryNode> Children => EmptyNodeList;
    }
}
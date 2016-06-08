using System;
using System.Collections.Generic;
using System.Text;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class TermNode : QueryNodeBase {
        public string Prefix { get; set; }
        public string Field { get; set; }
        public string Term { get; set; }
        public bool IsQuotedTerm { get; set; }
        public string Boost { get; set; }
        public string Proximity { get; set; }

        public TermNode CopyTo(TermNode target) {
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

            if (Boost != null)
                builder.Append("^" + Boost);

            if (Proximity != null)
                builder.Append("~" + Proximity);

            return builder.ToString();
        }

        public override IList<IQueryNode> Children => EmptyNodeList;
    }
}
using System;
using System.Collections.Generic;
using System.Text;
using Exceptionless.LuceneQueryParser.Extensions;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class TermNode : QueryNodeBase {
        public bool? IsNegated { get; set; }
        public string Prefix { get; set; }
        public string Field { get; set; }
        public string Term { get; set; }
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

            return target;
        }

        public override string ToString(bool escapeTerms)
        {
            var builder = new StringBuilder();

            if (IsNegated.HasValue && IsNegated.Value)
                builder.Append("NOT ");

            builder.Append(Prefix);

            if (!String.IsNullOrEmpty(Field))
            {
                builder.Append(Field);
                builder.Append(":");
            }

            var term = escapeTerms ? Term.Escape() : Term;
            builder.Append(IsQuotedTerm ? "\"" + term + "\"" : term);

            if (Boost != null)
                builder.Append("^" + Boost);

            if (Proximity != null)
                builder.Append("~" + Proximity);

            return builder.ToString();
        }

        public override IList<IQueryNode> Children => EmptyNodeList;
    }
}
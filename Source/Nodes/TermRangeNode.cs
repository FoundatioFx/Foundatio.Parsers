using System;
using System.Collections.Generic;
using System.Text;
using Exceptionless.LuceneQueryParser.Extensions;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class TermRangeNode : QueryNodeBase {
        public bool? IsNegated { get; set; }
        public string Field { get; set; }
        public string Prefix { get; set; }
        public string Min { get; set; }
        public string Max { get; set; }
        public string Operator { get; set; }
        public string Delimiter { get; set; }
        public bool? MinInclusive { get; set; }
        public bool? MaxInclusive { get; set; }

        public TermRangeNode CopyTo(TermRangeNode target) {
            if (Field != null)
                target.Field = Field;

            if (IsNegated.HasValue)
                target.IsNegated = IsNegated;

            if (Prefix != null)
                target.Prefix = Prefix;

            if (Min != null)
                target.Min = Min;

            if (Max != null)
                target.Max = Max;

            if (Operator != null)
                target.Operator = Operator;

            if (Delimiter != null)
                target.Delimiter = Delimiter;

            if (MinInclusive.HasValue)
                target.MinInclusive = MinInclusive;

            if (MaxInclusive.HasValue)
                target.MaxInclusive = MaxInclusive;

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

            if (!String.IsNullOrEmpty(Operator))
                builder.Append(Operator);

            if (MinInclusive.HasValue && String.IsNullOrEmpty(Operator))
                builder.Append(MinInclusive.Value ? "[" : "{");

            builder.Append(escapeTerms ? Min.Escape() : Min);

            if (!String.IsNullOrEmpty(Min) && !String.IsNullOrEmpty(Max) && String.IsNullOrEmpty(Operator))
                builder.Append(Delimiter ?? " TO ");

            builder.Append(escapeTerms ? Max.Escape() : Max);

            if (MaxInclusive.HasValue && String.IsNullOrEmpty(Operator))
                builder.Append(MaxInclusive.Value ? "]" : "}");

            return builder.ToString();
        }

        public override IList<IQueryNode> Children => EmptyNodeList;
    }
}
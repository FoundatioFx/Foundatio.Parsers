using System;
using System.Collections.Generic;
using System.Text;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class TermRangeNode : QueryNodeBase {
        public string Field { get; set; }
        public string Prefix { get; set; }
        public string Min { get; set; }
        public string Max { get; set; }
        public string Delimiter { get; set; }
        public bool? MinInclusive { get; set; }
        public bool? MaxInclusive { get; set; }

        public TermRangeNode CopyTo(TermRangeNode target) {
            if (Field != null)
                target.Field = Field;

            if (Prefix != null)
                target.Prefix = Prefix;

            if (Min != null)
                target.Min = Min;

            if (Max != null)
                target.Max = Max;

            if (Delimiter != null)
                target.Delimiter = Delimiter;

            if (MinInclusive.HasValue)
                target.MinInclusive = MinInclusive;

            if (MaxInclusive.HasValue)
                target.MaxInclusive = MaxInclusive;

            return target;
        }

        public override string ToString() {
            var builder = new StringBuilder();

            builder.Append(Prefix);

            if (!String.IsNullOrEmpty(Field)) {
                builder.Append(Prefix);
                builder.Append(Field);
                builder.Append(":");
            }

            if (MinInclusive.HasValue)
                builder.Append(MinInclusive.Value ? "[" : "{");

            builder.Append(Min);

            if (!String.IsNullOrEmpty(Min) && !String.IsNullOrEmpty(Max))
                builder.Append(Delimiter ?? " TO ");

            builder.Append(Max);

            if (MaxInclusive.HasValue)
                builder.Append(MaxInclusive.Value ? "]" : "}");

            return builder.ToString();
        }

        public override IList<IQueryNode> Children => EmptyNodeList;
    }
}
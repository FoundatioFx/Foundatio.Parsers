using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Extensions;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
    public class TermRangeNode : QueryNodeBase, IFieldQueryNode {
        public bool? IsNegated { get; set; }
        public string Field { get; set; }
        public string Prefix { get; set; }
        public string Min { get; set; }
        public string UnescapedMin => Min?.Unescape();
        public string Max { get; set; }
        public string UnescapedMax => Max?.Unescape();
        public string Operator { get; set; }
        public string Delimiter { get; set; }
        public bool? MinInclusive { get; set; }
        public bool? MaxInclusive { get; set; }
        public string Boost { get; set; }
        public string UnescapedBoost => Boost?.Unescape();
        public string Proximity { get; set; }

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

            if (Boost != null)
                target.Boost = Boost;

            if (Proximity != null)
                target.Proximity = Proximity;

            foreach (var kvp in Data)
                target.Data.Add(kvp.Key, kvp.Value);

            return target;
        }

        public override string ToString()
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

            builder.Append(Min);

            if (!String.IsNullOrEmpty(Min) && !String.IsNullOrEmpty(Max) && String.IsNullOrEmpty(Operator))
                builder.Append(Delimiter ?? " TO ");

            builder.Append(Max);

            if (MaxInclusive.HasValue && String.IsNullOrEmpty(Operator))
                builder.Append(MaxInclusive.Value ? "]" : "}");

            if (Proximity != null)
                builder.Append("~" + Proximity);

            if (Boost != null)
                builder.Append("^" + Boost);

            return builder.ToString();
        }

        public override IEnumerable<IQueryNode> Children => EmptyNodeList;
    }
}
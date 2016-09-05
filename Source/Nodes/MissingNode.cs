using System;
using System.Collections.Generic;
using System.Text;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class MissingNode : QueryNodeBase {
        public bool? IsNegated { get; set; }
        public string Prefix { get; set; }
        public string Field { get; set; }

        public MissingNode CopyTo(MissingNode target) {
            if (IsNegated.HasValue)
                target.IsNegated = IsNegated;

            if (Prefix != null)
                target.Prefix = Prefix;

            if (Field != null)
                target.Field = Field;

            return target;
        }

        public override string ToString(bool escapeTerms) {
            var builder = new StringBuilder();

            if (IsNegated.HasValue && IsNegated.Value)
                builder.Append("NOT ");

            builder.Append(Prefix);
            builder.Append("_missing_");
            builder.Append(":");
            builder.Append(Field);

            return builder.ToString();
        }

        public override IList<IQueryNode> Children => EmptyNodeList;
    }
}
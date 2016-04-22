using System;
using System.Collections.Generic;
using System.Text;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public class ExistsNode : QueryNodeBase {
        public string Prefix { get; set; }
        public string Field { get; set; }

        public ExistsNode CopyTo(ExistsNode target) {
            if (Prefix != null)
                target.Prefix = Prefix;

            if (Field != null)
                target.Field = Field;

            return target;
        }

        public override String ToString() {
            var builder = new StringBuilder();

            builder.Append(Prefix);
            builder.Append("_exists_");
            builder.Append(":");
            builder.Append(Field);

            return builder.ToString();
        }

        public override IList<IQueryNode> Children => EmptyNodeList;
    }
}
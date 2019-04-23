using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class GetReferencedFieldsQueryVisitor : QueryNodeVisitorWithResultBase<ISet<string>> {
        private readonly HashSet<string> _fields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            AddField(node, context);
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            AddField(node, context);
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            AddField(node, context);
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            AddField(node, context);
        }

        private void AddField(IFieldQueryNode node, IQueryVisitorContext context) {
            string field = node.Field;
            if (field != null)
                _fields.Add(field);
            else
                foreach (var defaultField in node.GetDefaultFields(context.DefaultFields))
                    _fields.Add(defaultField);
        }

        public override Task<ISet<string>> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            node.AcceptAsync(this, context);
            return Task.FromResult<ISet<string>>(_fields);
        }

        public static Task<ISet<string>> RunAsync(IQueryNode node, IQueryVisitorContext context = null) {
            return new GetReferencedFieldsQueryVisitor().AcceptAsync(node, context);
        }

        public static ISet<string> Run(IQueryNode node, IQueryVisitorContext context = null) {
            return RunAsync(node, context).GetAwaiter().GetResult();
        }
    }
}

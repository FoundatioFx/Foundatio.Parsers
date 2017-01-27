using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries {
    public partial class LuceneQueryParser : IQueryParser {
        public virtual Task<IQueryNode> ParseAsync(string query, string queryType = QueryType.Query, IQueryVisitorContext context = null) {
            var result = Parse(query);
            result.SetQueryType(queryType);

            return Task.FromResult<IQueryNode>(result);
        }

        public IQueryNode Parse(string query, string queryType, IQueryVisitorContext context) {
            return ParseAsync(query, queryType, context).GetAwaiter().GetResult();
        }
    }

    public interface IQueryParser {
        Task<IQueryNode> ParseAsync(string query, string queryType = QueryType.Query, IQueryVisitorContext context = null);
    }
}

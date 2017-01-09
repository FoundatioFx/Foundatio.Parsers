using System;
using System.Threading.Tasks;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorContextWithValidator : QueryVisitorContext, IQueryVisitorContextWithValidator {
        public Func<QueryValidationInfo, Task<bool>> Validator { get; set; }
        public QueryValidationInfo ValidationInfo { get; set; }
    }
}
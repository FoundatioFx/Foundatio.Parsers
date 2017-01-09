using System;
using System.Threading.Tasks;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryVisitorContextWithValidator : IQueryVisitorContext {
        Func<QueryValidationInfo, Task<bool>> Validator { get; set; }
        QueryValidationInfo ValidationInfo { get; set; }
    }
}
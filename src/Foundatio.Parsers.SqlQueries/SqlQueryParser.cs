using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Visitors;
using Pegasus.Common;

namespace Foundatio.Parsers.SqlQueries;

public class SqlQueryParser : LuceneQueryParser {
    public SqlQueryParser(Action<SqlQueryParserConfiguration> configure = null) {
        var config = new SqlQueryParserConfiguration();
        configure?.Invoke(config);
        Configuration = config;
    }

    public SqlQueryParserConfiguration Configuration { get; }
    
    public override async Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null) {
        query ??= String.Empty;

        if (context == null)
            context = new SqlQueryVisitorContext();

        //SetupQueryVisitorContextDefaults(context);
        try {
            var result = await base.ParseAsync(query, context).ConfigureAwait(false);
            switch (context.QueryType) {
                case QueryTypes.Aggregation:
                    result = await Configuration.AggregationVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryTypes.Query:
                    result = await Configuration.QueryVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
                case QueryTypes.Sort:
                    result = await Configuration.SortVisitor.AcceptAsync(result, context).ConfigureAwait(false);
                    break;
            }

            return result;
        } catch (FormatException ex) {
            var cursor = ex.Data["cursor"] as Cursor;
            context.GetValidationResult().QueryType = context.QueryType;
            context.AddValidationError(ex.Message, cursor.Column);

            return null;
        }
    }
}
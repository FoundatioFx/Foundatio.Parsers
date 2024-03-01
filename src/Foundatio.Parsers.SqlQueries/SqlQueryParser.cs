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

        SetupQueryVisitorContextDefaults(context);
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

    private void SetupQueryVisitorContextDefaults(IQueryVisitorContext context)
    {
        //context.SetMappingResolver(Configuration.MappingResolver);

        if (!context.Data.ContainsKey("@OriginalContextResolver"))
            context.SetValue("@OriginalContextResolver", context.GetFieldResolver());

        context.SetFieldResolver(async (field, context) =>
        {
            string resolvedField = null;
            if (context.Data.TryGetValue("@OriginalContextResolver", out var data) && data is QueryFieldResolver resolver)
            {
                var contextResolvedField = await resolver(field, context).ConfigureAwait(false);
                if (contextResolvedField != null)
                    resolvedField = contextResolvedField;
            }

            if (Configuration.FieldResolver != null)
            {
                var configResolvedField = await Configuration.FieldResolver(resolvedField ?? field, context).ConfigureAwait(false);
                if (configResolvedField != null)
                    resolvedField = configResolvedField;
            }

            //var mappingResolvedField = await MappingFieldResolver(resolvedField ?? field, context).ConfigureAwait(false);
            //if (mappingResolvedField != null)
            //    resolvedField = mappingResolvedField;

            return resolvedField;
        });

        if (Configuration.ValidationOptions != null && !context.HasValidationOptions())
            context.SetValidationOptions(Configuration.ValidationOptions);

        if (context.QueryType == QueryTypes.Query)
        {
            context.SetDefaultFields(Configuration.DefaultFields);
            if (Configuration.IncludeResolver != null && context.GetIncludeResolver() == null)
                context.SetIncludeResolver(Configuration.IncludeResolver);
        }
    }
}

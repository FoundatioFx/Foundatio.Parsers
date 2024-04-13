using System.Text;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.SqlQueries.Extensions;
using Foundatio.Parsers.SqlQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Tests;

public class DynamicFieldVisitor : ChainableMutatingQueryVisitor
{
    public override IQueryNode Visit(TermNode node, IQueryVisitorContext context)
    {
        if (context is not SqlQueryVisitorContext sqlContext)
            return node;

        var field = SqlNodeExtensions.GetFieldInfo(sqlContext.Fields, node.Field);

        if (field == null || !field.Data.TryGetValue("DataDefinitionId", out object value) ||
            value is not int dataDefinitionId)
        {
            return node;
        }

        var customFieldBuilder = new StringBuilder();

        customFieldBuilder.Append("DataValues.Any(DataDefinitionId = ");
        customFieldBuilder.Append(dataDefinitionId);
        customFieldBuilder.Append(" AND ");
        switch (field)
        {
            case { IsMoney: true }:
                customFieldBuilder.Append("MoneyValue");
                break;
            case { IsNumber: true }:
                customFieldBuilder.Append("NumberValue");
                break;
            case { IsBoolean: true }:
                customFieldBuilder.Append("BooleanValue");
                break;
            case { IsDate: true }:
                customFieldBuilder.Append("DateValue");
                break;
            default:
                customFieldBuilder.Append("StringValue");
                break;
        }

        customFieldBuilder.Append(" = ");
        if (field is { IsNumber: true } or { IsBoolean: true })
        {
            customFieldBuilder.Append(node.Term);
        }
        else
        {
            customFieldBuilder.Append("\"");
            customFieldBuilder.Append(node.Term);
            customFieldBuilder.Append("\"");
        }
        customFieldBuilder.Append(")");

        node.SetQuery(customFieldBuilder.ToString());

        return node;
    }
}

using System.Collections.Generic;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Extensions;

namespace Foundatio.Parsers.LuceneQueries.Nodes;

public class MissingNode : QueryNodeBase, IFieldQueryNode
{
    public bool? IsNegated { get; set; }
    public string Prefix { get; set; }
    public string Field { get; set; }
    public string UnescapedField => Field?.Unescape();

    public MissingNode CopyTo(MissingNode target)
    {
        if (IsNegated.HasValue)
            target.IsNegated = IsNegated;

        if (Prefix != null)
            target.Prefix = Prefix;

        if (Field != null)
            target.Field = Field;

        foreach (var kvp in Data)
            target.Data.Add(kvp.Key, kvp.Value);

        return target;
    }

    public override IQueryNode Clone()
    {
        var clone = new MissingNode();
        CopyTo(clone);
        return clone;
    }

    public override string ToString()
    {
        var builder = new StringBuilder();

        if (IsNegated.HasValue && IsNegated.Value)
            builder.Append("NOT ");

        builder.Append(Prefix);
        builder.Append("_missing_");
        builder.Append(":");
        builder.Append(this.Field);

        return builder.ToString();
    }

    public override IEnumerable<IQueryNode> Children => EmptyNodeList;
}

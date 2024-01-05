using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Extensions;

namespace Foundatio.Parsers.LuceneQueries.Nodes;

public class TermNode : QueryNodeBase, IFieldQueryWithProximityAndBoostNode
{
    public bool? IsNegated { get; set; }
    public string Prefix { get; set; }
    public string Field { get; set; }
    public string UnescapedField => Field?.Unescape();
    public string Term { get; set; }
    public string UnescapedTerm => Term?.Unescape();
    public bool IsQuotedTerm { get; set; }
    public bool IsRegexTerm { get; set; }
    public string Boost { get; set; }
    public string UnescapedBoost => Boost?.Unescape();
    public string Proximity { get; set; }
    public string UnescapedProximity => Proximity?.Unescape();

    public TermNode CopyTo(TermNode target)
    {
        if (IsNegated.HasValue)
            target.IsNegated = IsNegated;

        if (Prefix != null)
            target.Prefix = Prefix;

        if (Field != null)
            target.Field = Field;

        if (Term != null)
            target.Term = Term;

        target.IsQuotedTerm = IsQuotedTerm;
        target.IsRegexTerm = IsRegexTerm;

        if (Boost != null)
            target.Boost = Boost;

        if (Proximity != null)
            target.Proximity = Proximity;

        foreach (var kvp in Data)
            target.Data.Add(kvp.Key, kvp.Value);

        return target;
    }

    public override string ToString()
    {
        var builder = new StringBuilder();

        if (IsNegated.HasValue && IsNegated.Value)
            builder.Append("NOT ");

        builder.Append(Prefix);

        if (!String.IsNullOrEmpty(Field))
        {
            builder.Append(Field);
            builder.Append(":");
        }

        if (IsQuotedTerm)
            builder.Append("\"" + Term + "\"");
        else if (IsRegexTerm)
            builder.Append("/" + Term + "/");
        else
            builder.Append(Term);

        if (Proximity != null)
            builder.Append("~" + Proximity);

        if (Boost != null)
            builder.Append("^" + Boost);

        return builder.ToString();
    }

    public override IQueryNode Clone()
    {
        var clone = new TermNode();
        CopyTo(clone);
        return clone;
    }

    public override IEnumerable<IQueryNode> Children => EmptyNodeList;
}

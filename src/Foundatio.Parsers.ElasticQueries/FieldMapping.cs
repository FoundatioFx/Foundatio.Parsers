using System;
using System.Collections.Generic;
using Elastic.Clients.Elasticsearch.Mapping;

namespace Foundatio.Parsers.ElasticQueries;

public class FieldMapping
{
    public FieldMapping(string path, IProperty? property, DateTime? serverMapTime, long epoch = 0)
        : this(path, property, (MergedProperties?)null)
    {
    }

    internal FieldMapping(string path, IProperty? property)
        : this(path, property, (MergedProperties?)null)
    {
    }

    internal FieldMapping(string path, IProperty? property, MergedProperties? children)
    {
        FullPath = path;
        Property = property;
        Children = children;
    }

    public bool Found => Property is not null;
    public string FullPath { get; private set; }
    public IProperty? Property { get; private set; }
    public DateTime Date { get; private set; } = DateTime.UtcNow;

    /// <summary>
    /// Merged sub-objects and multi-fields of this field, captured during resolution so sub-field lookups do
    /// not have to walk the mapping again.
    /// </summary>
    internal MergedProperties? Children { get; }
}

/// <summary>A name-indexed merged view of the code and server properties at one mapping level.</summary>
internal sealed class MergedProperties
{
    private readonly IReadOnlyList<MergedNode> _nodes;
    private readonly Dictionary<string, MergedNode> _byExactName;
    private readonly Dictionary<string, MergedNode> _byIgnoreCaseName;

    public MergedProperties(IReadOnlyList<MergedNode> nodes)
    {
        _nodes = nodes;
        _byExactName = new Dictionary<string, MergedNode>(nodes.Count, StringComparer.Ordinal);
        _byIgnoreCaseName = new Dictionary<string, MergedNode>(nodes.Count, StringComparer.OrdinalIgnoreCase);

        foreach (var node in nodes)
        {
            _byExactName[node.Name] = node;
            _byIgnoreCaseName.TryAdd(node.Name, node);
        }
    }

    public int Count => _nodes.Count;

    public IReadOnlyList<MergedNode> Nodes => _nodes;

    public bool TryGetNode(string name, out MergedNode node)
    {
        return _byExactName.TryGetValue(name, out node!) || _byIgnoreCaseName.TryGetValue(name, out node!);
    }
}

/// <summary>A canonical property and the merged children reachable through its field name.</summary>
internal sealed class MergedNode
{
    public MergedNode(string name, IProperty property, MergedProperties? children)
    {
        Name = name;
        Property = property;
        Children = children;
    }

    public string Name { get; }

    public IProperty Property { get; }

    public MergedProperties? Children { get; }
}

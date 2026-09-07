using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;

namespace Foundatio.Parsers.ElasticQueries;

internal static class MappingProperty
{
    public static Properties? GetChildren(IProperty property) => property switch
    {
        ObjectProperty p => p.Properties,
        NestedProperty p => p.Properties,
        _ => property.GetFields()
    };

    public static IProperty WithChildren(IProperty property, MergedProperties? children)
    {
        if (children is null)
            return property;

        var original = GetChildren(property);
        if (original is not null && original.Count() == children.Count
            && original.All(pair => children.TryGetNode(pair.Key.Name!, out var child)
                && child.Name == pair.Key.Name && ReferenceEquals(pair.Value, child.Property)))
            return property;

        var properties = new Properties();
        foreach (var child in children.Nodes)
            properties.Add(child.Name, child.Property);

        // SDK properties have no copy constructor. A shallow copy preserves all client settings and field
        // expressions; only the child collection is replaced, without mutating the loader-owned property.
        var copy = (IProperty)MemberwiseClone(property);
        switch (copy)
        {
            case ObjectProperty p: p.Properties = properties; break;
            case NestedProperty p: p.Properties = properties; break;
            case AggregateMetricDoubleProperty p: p.Fields = properties; break;
            case BinaryProperty p: p.Fields = properties; break;
            case BooleanProperty p: p.Fields = properties; break;
            case ByteNumberProperty p: p.Fields = properties; break;
            case CompletionProperty p: p.Fields = properties; break;
            case ConstantKeywordProperty p: p.Fields = properties; break;
            case CountedKeywordProperty p: p.Fields = properties; break;
            case DateNanosProperty p: p.Fields = properties; break;
            case DateProperty p: p.Fields = properties; break;
            case DateRangeProperty p: p.Fields = properties; break;
            case DenseVectorProperty p: p.Fields = properties; break;
            case DoubleNumberProperty p: p.Fields = properties; break;
            case DoubleRangeProperty p: p.Fields = properties; break;
            case DynamicProperty p: p.Fields = properties; break;
            case FieldAliasProperty p: p.Fields = properties; break;
            case FlattenedProperty p: p.Fields = properties; break;
            case FloatNumberProperty p: p.Fields = properties; break;
            case FloatRangeProperty p: p.Fields = properties; break;
            case GeoPointProperty p: p.Fields = properties; break;
            case GeoShapeProperty p: p.Fields = properties; break;
            case HalfFloatNumberProperty p: p.Fields = properties; break;
            case HistogramProperty p: p.Fields = properties; break;
            case IcuCollationProperty p: p.Fields = properties; break;
            case IntegerNumberProperty p: p.Fields = properties; break;
            case IntegerRangeProperty p: p.Fields = properties; break;
            case IpProperty p: p.Fields = properties; break;
            case IpRangeProperty p: p.Fields = properties; break;
            case JoinProperty p: p.Fields = properties; break;
            case KeywordProperty p: p.Fields = properties; break;
            case LongNumberProperty p: p.Fields = properties; break;
            case LongRangeProperty p: p.Fields = properties; break;
            case MatchOnlyTextProperty p: p.Fields = properties; break;
            case Murmur3HashProperty p: p.Fields = properties; break;
            case PassthroughObjectProperty p: p.Fields = properties; break;
            case PercolatorProperty p: p.Fields = properties; break;
            case PointProperty p: p.Fields = properties; break;
            case RankFeatureProperty p: p.Fields = properties; break;
            case RankFeaturesProperty p: p.Fields = properties; break;
            case RankVectorProperty p: p.Fields = properties; break;
            case ScaledFloatNumberProperty p: p.Fields = properties; break;
            case SearchAsYouTypeProperty p: p.Fields = properties; break;
            case SemanticTextProperty p: p.Fields = properties; break;
            case ShapeProperty p: p.Fields = properties; break;
            case ShortNumberProperty p: p.Fields = properties; break;
            case SparseVectorProperty p: p.Fields = properties; break;
            case TextProperty p: p.Fields = properties; break;
            case TokenCountProperty p: p.Fields = properties; break;
            case UnsignedLongNumberProperty p: p.Fields = properties; break;
            case VersionProperty p: p.Fields = properties; break;
            case WildcardProperty p: p.Fields = properties; break;
#if ELASTICSEARCH9
            case ExponentialHistogramProperty p: p.Fields = properties; break;
#endif
            default: throw new NotSupportedException($"Cannot merge children of mapping property {property.GetType().Name}.");
        }

        return copy;
    }

    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "MemberwiseClone")]
    private static extern object MemberwiseClone(object instance);
}

using System;
using System.Collections.Generic;

namespace Foundatio.Parsers.SqlQueries.Extensions;

public static class TypeExtensions
{
    private static readonly IList<Type> _integerTypes = new List<Type>()
    {
        typeof (byte),
        typeof (short),
        typeof (int),
        typeof (long),
        typeof (sbyte),
        typeof (ushort),
        typeof (uint),
        typeof (ulong),
        typeof (byte?),
        typeof (short?),
        typeof (int?),
        typeof (long?),
        typeof (sbyte?),
        typeof (ushort?),
        typeof (uint?),
        typeof (ulong?)
    };

    public static Type UnwrapNullable(this Type type)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            return Nullable.GetUnderlyingType(type);

        return type;
    }

    public static bool IsString(this Type type) => type == typeof(string);
    public static bool IsDateTime(this Type typeToCheck) => typeToCheck == typeof(DateTime) || typeToCheck == typeof(DateTime?);
    public static bool IsBoolean(this Type typeToCheck) => typeToCheck == typeof(bool) || typeToCheck == typeof(bool?);
    public static bool IsNumeric(this Type type) => type.IsFloatingPoint() || type.IsIntegerBased();
    public static bool IsIntegerBased(this Type type) => _integerTypes.Contains(type);
    public static bool IsFloatingPoint(this Type type) => type == typeof(decimal) || type == typeof(float) || type == typeof(double);
}

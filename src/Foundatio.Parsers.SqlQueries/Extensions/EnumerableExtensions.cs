using System.Collections.Generic;

namespace Foundatio.Parsers.SqlQueries.Extensions;

internal static class EnumerableExtensions
{
    public delegate void ElementAction<in T>(T element, ElementInfo info);

    public static void ForEach<T>(this IEnumerable<T> elements, ElementAction<T> action)
    {
        using IEnumerator<T> enumerator = elements.GetEnumerator();
        bool isFirst = true;
        bool hasNext = enumerator.MoveNext();
        int index = 0;

        while (hasNext)
        {
            T current = enumerator.Current;
            hasNext = enumerator.MoveNext();
            action(current, new ElementInfo(index, isFirst, !hasNext));
            isFirst = false;
            index++;
        }
    }

    public struct ElementInfo
    {
        public ElementInfo(int index, bool isFirst, bool isLast)
            : this()
        {
            Index = index;
            IsFirst = isFirst;
            IsLast = isLast;
        }

        public int Index { get; private set; }
        public bool IsFirst { get; private set; }
        public bool IsLast { get; private set; }
    }
}


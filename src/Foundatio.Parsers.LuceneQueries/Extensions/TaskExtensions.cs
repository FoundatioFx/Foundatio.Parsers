using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Foundatio.Parsers;

internal static class TaskExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConfiguredTaskAwaitable AnyContext(this Task task)
    {
        return task.ConfigureAwait(false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConfiguredTaskAwaitable<T> AnyContext<T>(this Task<T> task)
    {
        return task.ConfigureAwait(false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConfiguredValueTaskAwaitable AnyContext(this ValueTask task)
    {
        return task.ConfigureAwait(false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ConfiguredValueTaskAwaitable<T> AnyContext<T>(this ValueTask<T> task)
    {
        return task.ConfigureAwait(false);
    }
}

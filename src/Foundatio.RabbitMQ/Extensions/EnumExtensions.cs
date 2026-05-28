using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.Serialization;

namespace Foundatio.Messaging;

internal static class EnumExtensions
{
    private static readonly ConcurrentDictionary<Enum, string> s_cache = new();

    public static string ToEnumString<T>(this T value) where T : struct, Enum
    {
        return s_cache.GetOrAdd(value, static v =>
        {
            var member = v.GetType().GetField(v.ToString()!);
            var attribute = member?.GetCustomAttribute<EnumMemberAttribute>();
            return attribute?.Value ?? v.ToString()!;
        });
    }
}

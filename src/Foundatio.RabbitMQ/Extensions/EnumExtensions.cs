using System;
using System.Reflection;
using System.Runtime.Serialization;

namespace Foundatio.Messaging;

internal static class EnumExtensions
{
    public static string ToEnumString<T>(this T value) where T : struct, Enum
    {
        var member = typeof(T).GetField(value.ToString()!);
        var attribute = member?.GetCustomAttribute<EnumMemberAttribute>();
        return attribute?.Value ?? value.ToString()!;
    }
}

using System;

namespace Foundatio.RabbitMQ.Tests;

internal static class TestInfrastructurePolicy
{
    internal const string RequiredVariable = "FOUNDATIO_RABBITMQ_REQUIRE_INFRASTRUCTURE";

    internal static bool Required => ResolveRequired(
        Environment.GetEnvironmentVariable(RequiredVariable),
        Environment.GetEnvironmentVariable("CI"),
        Environment.GetEnvironmentVariable("GITHUB_ACTIONS"));

    internal static bool ResolveRequired(string? configured, string? ci, string? githubActions)
    {
        bool explicitlyRequired = false;
        if (!String.IsNullOrWhiteSpace(configured) && !Boolean.TryParse(configured, out explicitlyRequired))
            throw new ArgumentException($"{RequiredVariable} must be true or false.", nameof(configured));

        return explicitlyRequired || IsEnabled(ci) || IsEnabled(githubActions);
    }

    internal static void ReportUnavailable(string resourceName, Exception exception, bool required)
    {
        if (required)
            throw new InvalidOperationException($"Required RabbitMQ test infrastructure '{resourceName}' is unavailable. Integration verification did not complete.", exception);

        Console.Error.WriteLine(
            $"OPTIONAL RabbitMQ test infrastructure '{resourceName}' is unavailable ({exception.GetType().Name}); dependent tests may be skipped. This is not integration verification. Set {RequiredVariable}=true to require infrastructure locally.");
    }

    private static bool IsEnabled(string? value) =>
        String.Equals(value, "true", StringComparison.OrdinalIgnoreCase) || String.Equals(value, "1", StringComparison.Ordinal);
}

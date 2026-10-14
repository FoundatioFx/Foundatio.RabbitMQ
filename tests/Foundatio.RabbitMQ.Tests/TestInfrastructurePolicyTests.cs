using System;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.RabbitMQ.Tests;

public class TestInfrastructurePolicyTests(ITestOutputHelper output) : TestWithLoggingBase(output)
{
    [Fact]
    public void ReportUnavailable_WhenOptional_DoesNotFailLocalRun()
    {
        // Arrange
        var cause = new TimeoutException("Synthetic readiness timeout");

        // Act
        var exception = Record.Exception(() => TestInfrastructurePolicy.ReportUnavailable(
            "test-broker", cause, required: false));

        // Assert
        Assert.Null(exception);
    }

    [Fact]
    public void ReportUnavailable_WhenRequired_PreservesFailureCause()
    {
        // Arrange
        var cause = new TimeoutException("Synthetic readiness timeout");

        // Act
        var exception = Assert.Throws<InvalidOperationException>(() =>
            TestInfrastructurePolicy.ReportUnavailable("test-broker", cause, required: true));

        // Assert
        Assert.Same(cause, exception.InnerException);
        Assert.Contains("test-broker", exception.Message);
    }

    [Theory]
    [InlineData(null, null, null, false)]
    [InlineData("", null, null, false)]
    [InlineData("false", null, null, false)]
    [InlineData("true", null, null, true)]
    [InlineData(null, "true", null, true)]
    [InlineData(null, "1", null, true)]
    [InlineData(null, null, "true", true)]
    [InlineData("false", "true", null, true)]
    [InlineData("false", null, "true", true)]
    [InlineData(null, "false", "false", false)]
    public void ResolveRequired_WithEnvironmentValues_PreservesCiRequirement(string? configured, string? ci, string? githubActions, bool expected)
    {
        // Arrange: environment values are supplied by the theory.

        // Act
        bool required = TestInfrastructurePolicy.ResolveRequired(configured, ci, githubActions);

        // Assert
        Assert.Equal(expected, required);
    }

    [Theory]
    [InlineData("tru")]
    [InlineData("optional")]
    [InlineData("0")]
    public void ResolveRequired_WithInvalidOverride_RejectsAmbiguousConfiguration(string configured)
    {
        // Arrange: the invalid override is supplied by the theory.

        // Act
        var exception = Record.Exception(() => TestInfrastructurePolicy.ResolveRequired(configured, null, null));

        // Assert
        Assert.IsType<ArgumentException>(exception);
    }
}

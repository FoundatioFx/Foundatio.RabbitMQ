using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Aspire.Hosting;
using Aspire.Hosting.Testing;

namespace Foundatio.RabbitMQ.ChaosTests;

public class ChaosTestHelper
{
    private readonly DistributedApplication _app;

    public ChaosTestHelper(DistributedApplication app)
    {
        _app = app;
    }

    public async Task FillDiskAsync(string resourceName, int sizeMB = 0, CancellationToken cancellationToken = default)
    {
        var containerId = await GetContainerIdAsync(resourceName, cancellationToken: cancellationToken);
        await DockerExecAsync(containerId,
            "rabbitmqctl set_disk_free_limit 999GB",
            cancellationToken);
    }

    public async Task ClearDiskAsync(string resourceName, CancellationToken cancellationToken = default)
    {
        var containerId = await GetContainerIdAsync(resourceName, cancellationToken: cancellationToken);
        await DockerExecAsync(containerId,
            "rabbitmqctl set_disk_free_limit 10MB",
            cancellationToken);
    }

    public async Task StopNodeAsync(string resourceName, CancellationToken cancellationToken = default)
    {
        var containerId = await GetContainerIdAsync(resourceName, includeExited: false, cancellationToken);
        await RunDockerCommandAsync($"kill {containerId}", cancellationToken);
    }

    public async Task StartNodeAsync(string resourceName, CancellationToken cancellationToken = default)
    {
        var containerId = await GetContainerIdAsync(resourceName, includeExited: true, cancellationToken);
        await RunDockerCommandAsync($"start {containerId}", cancellationToken);
    }

    public async Task<bool> HasDiskAlarmAsync(string resourceName, CancellationToken cancellationToken = default)
    {
        var containerId = await GetContainerIdAsync(resourceName, cancellationToken: cancellationToken);
        var output = await DockerExecAsync(containerId, "rabbitmqctl status --formatter json", cancellationToken);
        return output.Contains("disk", StringComparison.OrdinalIgnoreCase) &&
               output.Contains("\"resource\"", StringComparison.OrdinalIgnoreCase);
    }

    public async Task WaitForAlarmClearedAsync(string resourceName, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (!await HasDiskAlarmAsync(resourceName, cancellationToken))
                return;
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }
        throw new TimeoutException($"Disk alarm on '{resourceName}' did not clear within {timeout.TotalSeconds}s");
    }

    public async Task WaitForAlarmActiveAsync(string resourceName, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await HasDiskAlarmAsync(resourceName, cancellationToken))
                return;
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }
        throw new TimeoutException($"Disk alarm on '{resourceName}' did not activate within {timeout.TotalSeconds}s");
    }

    public async Task<string> GetClusterStatusAsync(string resourceName, CancellationToken cancellationToken = default)
    {
        var containerId = await GetContainerIdAsync(resourceName, cancellationToken: cancellationToken);
        return await DockerExecAsync(containerId, "rabbitmqctl cluster_status --formatter json", cancellationToken);
    }

    public string GetConnectionString(string resourceName)
    {
        var endpoint = _app.GetEndpoint(resourceName, "amqp");
        return $"amqp://guest:guest@{endpoint.Host}:{endpoint.Port}";
    }

    private async Task<string> GetContainerIdAsync(string resourceName, bool includeExited = false, CancellationToken cancellationToken = default)
    {
        var flags = includeExited ? "ps -aq" : "ps -q";
        var output = await RunDockerCommandAsync(
            $"{flags} --filter \"name=^{resourceName}$\"",
            cancellationToken);

        var containerId = output.Trim().Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        if (string.IsNullOrEmpty(containerId))
            throw new InvalidOperationException($"Container for resource '{resourceName}' not found");

        return containerId;
    }

    private static async Task<string> DockerExecAsync(string containerId, string command, CancellationToken cancellationToken)
    {
        return await RunDockerCommandAsync($"exec {containerId} sh -c \"{command}\"", cancellationToken);
    }

    private static async Task<string> RunDockerCommandAsync(string args, CancellationToken cancellationToken)
    {
        using var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = "docker",
            Arguments = args,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        process.Start();
        var outputTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var errorTask = process.StandardError.ReadToEndAsync(cancellationToken);
        await Task.WhenAll(outputTask, errorTask);
        await process.WaitForExitAsync(cancellationToken);

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"docker {args} failed (exit code {process.ExitCode}): {errorTask.Result.Trim()}");

        return outputTask.Result;
    }
}

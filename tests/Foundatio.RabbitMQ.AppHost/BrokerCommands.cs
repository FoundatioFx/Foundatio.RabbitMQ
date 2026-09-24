using System.Diagnostics;
using System.Globalization;

internal sealed class BrokerCommands(string containerName)
{
    private readonly SemaphoreSlim _alarmLock = new(1, 1);
    private readonly Dictionary<bool, string> _previousLimits = new();

    internal async Task SetAlarmAsync(bool memory, bool active, CancellationToken cancellationToken)
    {
        await _alarmLock.WaitAsync(cancellationToken);
        try
        {
            if (active)
            {
                if (!_previousLimits.ContainsKey(memory))
                {
                    string expression = memory ? "vm_memory_monitor:get_memory_limit()." : "rabbit_disk_monitor:get_disk_free_limit().";
                    string value = (await ExecuteAsync(["rabbitmqctl", "eval", expression], cancellationToken)).Trim();
                    if (!Int64.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out long limit) || limit < 0)
                        throw new InvalidOperationException("Could not capture the broker's current limit; no alarm was changed.");
                    _previousLimits.Add(memory, value);
                }
                await ExecuteAsync(memory
                    ? ["rabbitmqctl", "set_vm_memory_high_watermark", "absolute", "1"]
                    : ["rabbitmqctl", "set_disk_free_limit", "999GB"], cancellationToken);
            }
            else if (_previousLimits.TryGetValue(memory, out string? previous))
            {
                await ExecuteAsync(memory
                    ? ["rabbitmqctl", "set_vm_memory_high_watermark", "absolute", previous]
                    : ["rabbitmqctl", "set_disk_free_limit", previous], cancellationToken);
                _previousLimits.Remove(memory);
            }
        }
        finally
        {
            _alarmLock.Release();
        }
    }

    internal async Task<string> ExecuteAsync(string[] arguments, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo("docker")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };
        process.StartInfo.ArgumentList.Add("exec");
        process.StartInfo.ArgumentList.Add(containerName);
        foreach (string argument in arguments)
            process.StartInfo.ArgumentList.Add(argument);
        process.Start();
        var output = process.StandardOutput.ReadToEndAsync(timeout.Token);
        var error = process.StandardError.ReadToEndAsync(timeout.Token);
        try
        {
            await process.WaitForExitAsync(timeout.Token);
            await Task.WhenAll(output, error);
            if (process.ExitCode != 0)
                throw new InvalidOperationException($"Broker command failed: {(await error).Trim()}");
            return await output;
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            throw;
        }
    }
}

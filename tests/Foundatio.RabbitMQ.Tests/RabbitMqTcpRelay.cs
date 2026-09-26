using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Foundatio.RabbitMQ.Tests;

internal sealed class RabbitMqTcpRelay : IAsyncDisposable
{
    private readonly Uri _backend;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stop = new();
    private readonly List<Task> _tunnels = [];
    private readonly Task _acceptTask;
    private readonly object _disposeLock = new();
    private Task? _disposeTask;
    public int Port { get; }
    public bool IsStopped => _stop.IsCancellationRequested;

    internal RabbitMqTcpRelay(Uri backend, IPAddress address, int port = 0)
    {
        _backend = backend;
        _listener = new TcpListener(address, port);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptTask = AcceptAsync();
    }

    private async Task AcceptAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                _tunnels.Add(ForwardAsync(client));
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (SocketException) when (_stop.IsCancellationRequested) { }
        finally
        {
            await _stop.CancelAsync();
            await Task.WhenAll(_tunnels);
        }
    }

    private async Task ForwardAsync(TcpClient client)
    {
        using (client)
        using (var upstream = new TcpClient())
        using (var stopped = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token))
        {
            try
            {
                await upstream.ConnectAsync(_backend.DnsSafeHost, _backend.Port, stopped.Token);
                var downstreamStream = client.GetStream();
                var upstreamStream = upstream.GetStream();
                var outbound = downstreamStream.CopyToAsync(upstreamStream, stopped.Token);
                var inbound = upstreamStream.CopyToAsync(downstreamStream, stopped.Token);
                await Task.WhenAny(outbound, inbound);
                await stopped.CancelAsync();
                try
                {
                    await Task.WhenAll(outbound, inbound);
                }
                catch (OperationCanceledException) when (stopped.IsCancellationRequested) { }
                catch (IOException) when (stopped.IsCancellationRequested) { }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
            catch (IOException) when (_stop.IsCancellationRequested) { }
            catch (ObjectDisposedException) when (_stop.IsCancellationRequested) { }
            catch (SocketException) when (_stop.IsCancellationRequested) { }
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_disposeLock)
            return new ValueTask(_disposeTask ??= DisposeCoreAsync());
    }

    private async Task DisposeCoreAsync()
    {
        try
        {
            await _stop.CancelAsync();
            _listener.Stop();
            await _acceptTask;
        }
        finally
        {
            _listener.Stop();
            _stop.Dispose();
        }
    }
}

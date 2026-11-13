using System;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace Foundatio.RabbitMQ.Tests;

internal sealed class RabbitMqTestCertificates : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"foundatio-rabbitmq-{Guid.NewGuid():N}");
    private X509Certificate2? _trustedRoot;
    private bool _trusted;

    internal string TrustedDirectory => Path.Combine(_directory, "trusted");
    internal string UntrustedDirectory => Path.Combine(_directory, "untrusted");
    internal string Password { get; } = Convert.ToHexString(RandomNumberGenerator.GetBytes(24));

    private RabbitMqTestCertificates() { }

    internal static async Task<RabbitMqTestCertificates> CreateAsync(CancellationToken cancellationToken)
    {
        var certificates = new RabbitMqTestCertificates();
        try
        {
            await certificates.InitializeAsync(cancellationToken);
            return certificates;
        }
        catch
        {
            certificates.Dispose();
            throw;
        }
    }

    private async Task InitializeAsync(CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(_directory);
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(_directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        _trustedRoot = await CreateCertificatesAsync(TrustedDirectory, cancellationToken);
        using var untrustedRoot = await CreateCertificatesAsync(UntrustedDirectory, cancellationToken);
        using var store = new X509Store(StoreName.Root, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadWrite);
        store.Add(_trustedRoot);
        _trusted = true;
    }

    private static async Task<X509Certificate2> CreateCertificatesAsync(string directory, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(directory);
        using var rootKey = RSA.Create(2048);
        var rootRequest = new CertificateRequest($"CN=Foundatio RabbitMQ test {Guid.NewGuid():N}", rootKey,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        rootRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        rootRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(rootRequest.PublicKey, false));
        var notBefore = DateTimeOffset.FromUnixTimeSeconds(DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeSeconds());
        var notAfter = notBefore.AddDays(2);
        using var root = rootRequest.CreateSelfSigned(notBefore, notAfter);

        using var serverKey = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", serverKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.1") }, true));
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        names.AddIpAddress(IPAddress.IPv6Loopback);
        request.CertificateExtensions.Add(names.Build());
        using var server = request.Create(root, notBefore, notAfter, RandomNumberGenerator.GetBytes(16));
        await File.WriteAllTextAsync(Path.Combine(directory, "ca.crt"), root.ExportCertificatePem(), cancellationToken);
        await File.WriteAllTextAsync(Path.Combine(directory, "server.crt"), server.ExportCertificatePem(), cancellationToken);
        await File.WriteAllTextAsync(Path.Combine(directory, "server.key"), serverKey.ExportPkcs8PrivateKeyPem(), cancellationToken);
        await File.WriteAllTextAsync(Path.Combine(directory, "rabbitmq.conf"), """
            listeners.tcp = none
            listeners.ssl.default = 5671
            ssl_options.cacertfile = /certificates/ca.crt
            ssl_options.certfile = /certificates/server.crt
            ssl_options.keyfile = /certificates/server.key
            ssl_options.verify = verify_peer
            ssl_options.fail_if_no_peer_cert = false
            """, cancellationToken);
        if (!OperatingSystem.IsWindows())
        {
            // The private parent directory protects the files on the host. The mounted
            // child must be readable by the broker's unprivileged container identity.
            File.SetUnixFileMode(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute
                | UnixFileMode.GroupRead | UnixFileMode.GroupExecute | UnixFileMode.OtherRead | UnixFileMode.OtherExecute);
            foreach (string path in Directory.GetFiles(directory))
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead | UnixFileMode.OtherRead);
        }
        return X509CertificateLoader.LoadCertificate(root.RawData);
    }

    public void Dispose()
    {
        try
        {
            if (_trusted && _trustedRoot is not null)
            {
                using var store = new X509Store(StoreName.Root, StoreLocation.CurrentUser);
                store.Open(OpenFlags.ReadWrite);
                store.Remove(_trustedRoot);
                _trusted = false;
            }
        }
        finally
        {
            _trustedRoot?.Dispose();
            _trustedRoot = null;
            if (Directory.Exists(_directory))
                Directory.Delete(_directory, recursive: true);
        }
    }
}

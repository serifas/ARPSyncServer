using ByteSizeLib;
using MareSynchronosShared.Data;
using MareSynchronosShared.Metrics;
using MareSynchronosShared.Models;
using MareSynchronosShared.Services;
using MareSynchronosStaticFilesServer.Utils;
using Microsoft.EntityFrameworkCore;

namespace MareSynchronosStaticFilesServer.Services;

public class FileCleanupService : IHostedService
{
    private readonly IConfigurationService<StaticFilesServerConfiguration> _configuration;
    private readonly ILogger<FileCleanupService> _logger;
    private readonly MareMetrics _metrics;
    private readonly IServiceProvider _services;

    private readonly string _hotStoragePath;
    private readonly string _coldStoragePath;
    private readonly bool _isMain = false;
    private readonly bool _isDistributionNode = false;
    private readonly bool _useColdStorage = false;

    private CancellationTokenSource _cleanupCts;

    private int HotStorageRetention => _configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.UnusedFileRetentionPeriodInDays), 14);
    private double HotStorageSize => _configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.CacheSizeHardLimitInGiB), -1.0);

    private int ColdStorageRetention => _configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.ColdStorageUnusedFileRetentionPeriodInDays), 60);
    private double ColdStorageSize => _configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.ColdStorageSizeHardLimitInGiB), -1.0);

    private int ForcedDeletionAfterHours => _configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.ForcedDeletionOfFilesAfterHours), -1);
    private int CleanupCheckMinutes => _configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.CleanupCheckInMinutes), 15);

    private List<FileInfo> GetAllHotFiles() => new DirectoryInfo(_hotStoragePath).GetFiles("*", SearchOption.AllDirectories)
                .Where(f => f != null && f.Name.Length == 40)
                .OrderBy(f => f.LastAccessTimeUtc).ToList();

    private List<FileInfo> GetAllColdFiles() => new DirectoryInfo(_coldStoragePath).GetFiles("*", SearchOption.AllDirectories)
                .Where(f => f != null && f.Name.Length == 40)
                .OrderBy(f => f.LastAccessTimeUtc).ToList();

    private List<FileInfo> GetTempFiles() => new DirectoryInfo(_useColdStorage ? _coldStoragePath : _hotStoragePath).GetFiles("*", SearchOption.AllDirectories)
                .Where(f => f != null && (f.Name.EndsWith(".dl", StringComparison.InvariantCultureIgnoreCase) || f.Name.EndsWith(".tmp", StringComparison.InvariantCultureIgnoreCase))).ToList();

    public FileCleanupService(MareMetrics metrics, ILogger<FileCleanupService> logger,
        IServiceProvider services, IConfigurationService<StaticFilesServerConfiguration> configuration)
    {
        _metrics = metrics;
        _logger = logger;
        _services = services;
        _configuration = configuration;
        _useColdStorage = _configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.UseColdStorage), false);
        _hotStoragePath = configuration.GetValue<string>(nameof(StaticFilesServerConfiguration.CacheDirectory));
        _coldStoragePath = configuration.GetValue<string>(nameof(StaticFilesServerConfiguration.ColdStorageDirectory));
        _isDistributionNode = configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.IsDistributionNode), false);
        _isMain = configuration.GetValue<Uri>(nameof(StaticFilesServerConfiguration.MainFileServerAddress)) == null && _isDistributionNode;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Cleanup Service started");

        InitializeGauges();

        _cleanupCts = new();

        _ = CleanUpTask(_cleanupCts.Token);

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cleanupCts.Cancel();

        return Task.CompletedTask;
    }

    private List<string> CleanUpFilesBeyondSizeLimit(List<FileInfo> files, double sizeLimit, CancellationToken ct)
    {
        var removedFiles = new List<string>();
        if (sizeLimit <= 0)
        {
            return removedFiles;
        }

        try
        {
            _logger.LogInformation("Cleaning up files beyond the cache size limit of {cacheSizeLimit} GiB", sizeLimit);
            var allLocalFiles = files;
            var totalCacheSizeInBytes = allLocalFiles.Sum(s => s.Length);
            long cacheSizeLimitInBytes = (long)ByteSize.FromGibiBytes(sizeLimit).Bytes;
            while (totalCacheSizeInBytes > cacheSizeLimitInBytes && allLocalFiles.Count != 0 && !ct.IsCancellationRequested)
            {
                var oldestFile = allLocalFiles[0];
                allLocalFiles.RemoveAt(0);
                totalCacheSizeInBytes -= oldestFile.Length;
                _logger.LogInformation("Deleting {oldestFile} with size {size}MiB", oldestFile.FullName, ByteSize.FromBytes(oldestFile.Length).MebiBytes);
                oldestFile.Delete();
                removedFiles.Add(oldestFile.Name);
            }
            files.RemoveAll(f => removedFiles.Contains(f.Name, StringComparer.InvariantCultureIgnoreCase));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during cache size limit cleanup");
        }

        return removedFiles;
    }

    private void CleanUpOrphanedFiles(HashSet<string> allDbFileHashes, List<FileInfo> allPhysicalFiles, CancellationToken ct)
    {
        foreach (var file in allPhysicalFiles.ToList())
        {
            if (!allDbFileHashes.Contains(file.Name.ToUpperInvariant()))
            {
                file.Delete();
                _logger.LogInformation("File not in DB, deleting: {fileName}", file.Name);
                allPhysicalFiles.Remove(file);
            }

            ct.ThrowIfCancellationRequested();
        }
    }

    private List<string> CleanUpOutdatedFiles(List<FileInfo> files, int unusedRetention, int forcedDeletionAfterHours, CancellationToken ct)
    {
        var removedFiles = new List<string>();
        try
        {
            _logger.LogInformation("Cleaning up files older than {filesOlderThanDays} days", unusedRetention);
            if (forcedDeletionAfterHours > 0)
            {
                _logger.LogInformation("Cleaning up files written to longer than {hours}h ago", forcedDeletionAfterHours);
            }

            var lastAccessCutoffTime = DateTime.Now.Subtract(TimeSpan.FromDays(unusedRetention));
            var forcedDeletionCutoffTime = DateTime.Now.Subtract(TimeSpan.FromHours(forcedDeletionAfterHours));

            foreach (var file in files)
            {
                if (file.LastAccessTime < lastAccessCutoffTime)
                {
                    _logger.LogInformation("File outdated: {fileName}, {fileSize}MiB", file.Name, ByteSize.FromBytes(file.Length).MebiBytes);
                    file.Delete();
                    removedFiles.Add(file.Name);
                }
                else if (forcedDeletionAfterHours > 0 && file.LastWriteTime < forcedDeletionCutoffTime)
                {
                    _logger.LogInformation("File forcefully deleted: {fileName}, {fileSize}MiB", file.Name, ByteSize.FromBytes(file.Length).MebiBytes);
                    file.Delete();
                    removedFiles.Add(file.Name);
                }

                ct.ThrowIfCancellationRequested();
            }
            files.RemoveAll(f => removedFiles.Contains(f.Name, StringComparer.InvariantCultureIgnoreCase));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during file cleanup of old files");
        }

        return removedFiles;
    }

    private void CleanUpTempFiles()
    {
        var pastTime = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(20));
        var tempFiles = GetTempFiles();
        foreach (var tempFile in tempFiles.Where(f => f.LastWriteTimeUtc < pastTime))
            tempFile.Delete();
    }

    private async Task CleanUpTask(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var scope = _services.CreateScope();
                using var dbContext = _isMain ? scope.ServiceProvider.GetService<MareDbContext>()! : null;

                HashSet<string> allDbFileHashes = null;

                // Database operations only performed on main server
                if (_isMain)
                {
                    var allDbFiles = await dbContext.Files.ToListAsync(ct).ConfigureAwait(false);
                    allDbFileHashes = new HashSet<string>(allDbFiles.Select(a => a.Hash.ToUpperInvariant()), StringComparer.Ordinal);
                }

                if (_useColdStorage)
                {
                    var coldFiles = GetAllColdFiles();
                    var removedColdFiles = new List<string>();

                    removedColdFiles.AddRange(
                        CleanUpOutdatedFiles(coldFiles, ColdStorageRetention, ForcedDeletionAfterHours, ct)
                    );
                    removedColdFiles.AddRange(
                        CleanUpFilesBeyondSizeLimit(coldFiles, ColdStorageSize, ct)
                    );

                    // Remove cold storage files are deleted from the database, if we are the main file server
                    if (_isMain)
                    {
                        dbContext.Files.RemoveRange(
                            dbContext.Files.Where(f => removedColdFiles.Contains(f.Hash))
                        );
                        allDbFileHashes.ExceptWith(removedColdFiles);
                        CleanUpOrphanedFiles(allDbFileHashes, coldFiles, ct);
                    }

                    // Remove hot copies of files now that the authoritative copy is gone
                    foreach (var removedFile in removedColdFiles)
                    {
                        var hotFile = FilePathUtil.GetFileInfoForHash(_hotStoragePath, removedFile);
                        hotFile?.Delete();
                    }

                    _metrics.SetGaugeTo(MetricsAPI.GaugeFilesTotalSizeColdStorage, coldFiles.Sum(f => { try { return f.Length; } catch { return 0; } }));
                    _metrics.SetGaugeTo(MetricsAPI.GaugeFilesTotalColdStorage, coldFiles.Count);
                }

                var hotFiles = GetAllHotFiles();
                var removedHotFiles = new List<string>();

                removedHotFiles.AddRange(
                    CleanUpOutdatedFiles(hotFiles, HotStorageRetention, forcedDeletionAfterHours: _useColdStorage ? ForcedDeletionAfterHours : -1, ct)
                );
                removedHotFiles.AddRange(
                    CleanUpFilesBeyondSizeLimit(hotFiles, HotStorageSize, ct)
                );

                if (_isMain)
                {
                    // If cold storage is not active, then "hot" files are deleted from the database instead
                    if (!_useColdStorage)
                    {
                        dbContext.Files.RemoveRange(
                            dbContext.Files.Where(f => removedHotFiles.Contains(f.Hash))
                        );
                        allDbFileHashes.ExceptWith(removedHotFiles);
                    }

                    CleanUpOrphanedFiles(allDbFileHashes, hotFiles, ct);

                    await dbContext.SaveChangesAsync(ct).ConfigureAwait(false);
                }

                _metrics.SetGaugeTo(MetricsAPI.GaugeFilesTotalSize, hotFiles.Sum(f => { try { return f.Length; } catch { return 0; } }));
                _metrics.SetGaugeTo(MetricsAPI.GaugeFilesTotal, hotFiles.Count);

                CleanUpTempFiles();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error during cleanup task");
            }

            var cleanupCheckMinutes = CleanupCheckMinutes;
            var now = DateTime.Now;
            TimeOnly currentTime = new(now.Hour, now.Minute, now.Second);
            TimeOnly futureTime = new(now.Hour, now.Minute - now.Minute % cleanupCheckMinutes, 0);
            var span = futureTime.AddMinutes(cleanupCheckMinutes) - currentTime;

            _logger.LogInformation("File Cleanup Complete, next run at {date}", now.Add(span));
            await Task.Delay(span, ct).ConfigureAwait(false);
        }
    }

    private void InitializeGauges()
    {
        if (_useColdStorage)
        {
            var allFilesInColdStorageDir = GetAllColdFiles();

            _metrics.SetGaugeTo(MetricsAPI.GaugeFilesTotalSizeColdStorage, allFilesInColdStorageDir.Sum(f => f.Length));
            _metrics.SetGaugeTo(MetricsAPI.GaugeFilesTotalColdStorage, allFilesInColdStorageDir.Count);
        }

        var allFilesInHotStorage = GetAllHotFiles();

        _metrics.SetGaugeTo(MetricsAPI.GaugeFilesTotalSize, allFilesInHotStorage.Sum(f => { try { return f.Length; } catch { return 0; } }));
        _metrics.SetGaugeTo(MetricsAPI.GaugeFilesTotal, allFilesInHotStorage.Count);
    }
}
using MareSynchronosShared.Services;
using MareSynchronosStaticFilesServer.Utils;

namespace MareSynchronosStaticFilesServer.Services;

// Perform access time updates for cold cache files accessed via hot cache or shard servers
public class ColdTouchHashService : ITouchHashService
{
    private readonly ILogger<ColdTouchHashService> _logger;
    private readonly IConfigurationService<StaticFilesServerConfiguration> _configuration;

    private readonly bool _useColdStorage;
	private readonly string _coldStoragePath;

	// Debounce multiple updates towards the same file
	private readonly Dictionary<string, DateTime> _lastUpdateTimesUtc = new(1009, StringComparer.Ordinal);
	private int _cleanupCounter = 0;
	private const double _debounceTimeSecs = 90.0;

    public ColdTouchHashService(ILogger<ColdTouchHashService> logger, IConfigurationService<StaticFilesServerConfiguration> configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _useColdStorage = configuration.GetValueOrDefault(nameof(StaticFilesServerConfiguration.UseColdStorage), false);
        _coldStoragePath = configuration.GetValue<string>(nameof(StaticFilesServerConfiguration.ColdStorageDirectory));
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public void TouchColdHash(string hash)
    {
		if (!_useColdStorage)
			return;

		var nowUtc = DateTime.UtcNow;

		// Clean up debounce dictionary regularly
		if (_cleanupCounter++ >= 1000)
		{
			foreach (var entry in _lastUpdateTimesUtc.Where(entry => (nowUtc - entry.Value).TotalSeconds >= _debounceTimeSecs).ToList())
				_lastUpdateTimesUtc.Remove(entry.Key);
            _cleanupCounter = 0;
		}

		// Ignore multiple updates within a 90 second window of the first
		if (_lastUpdateTimesUtc.TryGetValue(hash, out var lastUpdateTimeUtc) && (nowUtc - lastUpdateTimeUtc).TotalSeconds < _debounceTimeSecs)
        {
            _logger.LogDebug($"Debounced touch for {hash}");
			return;
        }

        var fileInfo = FilePathUtil.GetFileInfoForHash(_coldStoragePath, hash);
        if (fileInfo != null)
        {
            _logger.LogDebug($"Touching {fileInfo.Name}");
		    fileInfo.LastAccessTimeUtc = nowUtc;
            _lastUpdateTimesUtc.TryAdd(hash, nowUtc);
        }
    }
}

using MareSynchronos.API.Routes;
using MareSynchronosStaticFilesServer.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace MareSynchronosStaticFilesServer.Controllers;

[Route(MareFiles.Distribution)]
public class DistributionController : ControllerBase
{
    private readonly CachedFileProvider _cachedFileProvider;

    public DistributionController(ILogger<DistributionController> logger, CachedFileProvider cachedFileProvider) : base(logger)
    {
        _cachedFileProvider = cachedFileProvider;
    }

    [HttpGet(MareFiles.Distribution_Get)]
    [Authorize(Policy = "Internal")]
    public async Task<IActionResult> GetFile(string file)
    {
        _logger.LogInformation($"GetFile:{MareUser}:{file}");

        var fs = await _cachedFileProvider.GetAndDownloadFileStream(file);
        if (fs == null) return NotFound();

        return File(fs, "application/octet-stream");
    }

    [HttpPost("touch")]
    [Authorize(Policy = "Internal")]
    public IActionResult TouchFiles([FromBody] string[] files)
    {
        _logger.LogInformation($"TouchFiles:{MareUser}:{files.Length}");

        if (files.Length == 0)
            return Ok();

        Task.Run(() => {
            foreach (var file in files)
                _cachedFileProvider.TouchColdHash(file);
        }).ConfigureAwait(false);

        return Ok();
    }
}

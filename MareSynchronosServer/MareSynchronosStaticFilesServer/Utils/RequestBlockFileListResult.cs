using MareSynchronosShared.Metrics;
using MareSynchronosStaticFilesServer.Services;
using Microsoft.AspNetCore.Mvc;
using System.Globalization;
using System.Text;

namespace MareSynchronosStaticFilesServer.Utils;

public class RequestBlockFileListResult : IActionResult
{
    private readonly Guid _requestId;
    private readonly RequestQueueService _requestQueueService;
    private readonly MareMetrics _mareMetrics;
    private readonly IEnumerable<FileInfo> _fileList;

    public RequestBlockFileListResult(Guid requestId, RequestQueueService requestQueueService, MareMetrics mareMetrics, IEnumerable<FileInfo> fileList)
    {
        _requestId = requestId;
        _requestQueueService = requestQueueService;
        _mareMetrics = mareMetrics;
        _mareMetrics.IncGauge(MetricsAPI.GaugeCurrentDownloads);
        _fileList = fileList;
    }

    public async Task ExecuteResultAsync(ActionContext context)
    {
        try
        {
            ArgumentNullException.ThrowIfNull(context);

            context.HttpContext.Response.StatusCode = 200;
            context.HttpContext.Response.ContentType = "application/octet-stream";

            foreach (var file in _fileList)
            {
                await context.HttpContext.Response.WriteAsync("#" + file.Name + ":" + file.Length.ToString(CultureInfo.InvariantCulture) + "#", Encoding.ASCII);
                await context.HttpContext.Response.SendFileAsync(file.FullName);
            }
        }
        catch
        {
            throw;
        }
        finally
        {
            _requestQueueService.FinishRequest(_requestId);
            _mareMetrics.DecGauge(MetricsAPI.GaugeCurrentDownloads);
        }
    }
}
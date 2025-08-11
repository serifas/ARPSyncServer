﻿using MareSynchronos.API.Data;
using MareSynchronos.API.Data.Enum;
using MareSynchronos.API.Dto;
using MareSynchronos.API.SignalR;
using MareSynchronosServer.Services;
using MareSynchronosServer.Utils;
using MareSynchronosShared;
using MareSynchronosShared.Data;
using MareSynchronosShared.Metrics;
using MareSynchronosShared.Services;
using MareSynchronosShared.Utils.Configuration;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis.Extensions.Core.Abstractions;

namespace MareSynchronosServer.Hubs;

[Authorize(Policy = "Authenticated")]
public partial class MareHub : Hub<IMareHub>, IMareHub
{
    private readonly MareMetrics _mareMetrics;
    private readonly SystemInfoService _systemInfoService;
    private readonly IHttpContextAccessor _contextAccessor;
    private readonly MareHubLogger _logger;
    private readonly string _shardName;
    private readonly int _maxExistingGroupsByUser;
    private readonly int _maxJoinedGroupsByUser;
    private readonly int _maxGroupUserCount;
    private readonly IRedisDatabase _redis;
    private readonly GPoseLobbyDistributionService _gPoseLobbyDistributionService;
    private readonly Uri _fileServerAddress;
    private readonly Version _expectedClientVersion;
    private readonly int _maxCharaDataByUser;

    private readonly Lazy<MareDbContext> _dbContextLazy;
    private MareDbContext DbContext => _dbContextLazy.Value;

    public MareHub(MareMetrics mareMetrics,
        IDbContextFactory<MareDbContext> mareDbContextFactory, ILogger<MareHub> logger, SystemInfoService systemInfoService,
        IConfigurationService<ServerConfiguration> configuration, IHttpContextAccessor contextAccessor,
        IRedisDatabase redisDb, GPoseLobbyDistributionService gPoseLobbyDistributionService)
    {
        _mareMetrics = mareMetrics;
        _systemInfoService = systemInfoService;
        _shardName = configuration.GetValue<string>(nameof(ServerConfiguration.ShardName));
        _maxExistingGroupsByUser = configuration.GetValueOrDefault(nameof(ServerConfiguration.MaxExistingGroupsByUser), 3);
        _maxJoinedGroupsByUser = configuration.GetValueOrDefault(nameof(ServerConfiguration.MaxJoinedGroupsByUser), 6);
        _maxGroupUserCount = configuration.GetValueOrDefault(nameof(ServerConfiguration.MaxGroupUserCount), 100);
        _fileServerAddress = configuration.GetValue<Uri>(nameof(ServerConfiguration.CdnFullUrl));
        _expectedClientVersion = configuration.GetValueOrDefault(nameof(ServerConfiguration.ExpectedClientVersion), new Version(0, 0, 0));
        _maxCharaDataByUser = configuration.GetValueOrDefault(nameof(ServerConfiguration.MaxCharaDataByUser), 10);
        _contextAccessor = contextAccessor;
        _redis = redisDb;
        _gPoseLobbyDistributionService = gPoseLobbyDistributionService;
        _logger = new MareHubLogger(this, logger);
        _dbContextLazy = new Lazy<MareDbContext>(() => mareDbContextFactory.CreateDbContext());
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DbContext.Dispose();
        }

        base.Dispose(disposing);
    }

    [Authorize(Policy = "Identified")]
    public async Task<ConnectionDto> GetConnectionDto()
    {
        _logger.LogCallInfo();

        _mareMetrics.IncCounter(MetricsAPI.CounterInitializedConnections);

        await Clients.Caller.Client_UpdateSystemInfo(_systemInfoService.SystemInfoDto).ConfigureAwait(false);

        var dbUser = DbContext.Users.SingleOrDefault(f => f.UID == UserUID);
        dbUser.LastLoggedIn = DateTime.UtcNow;
        await DbContext.SaveChangesAsync().ConfigureAwait(false);

        await Clients.Caller.Client_ReceiveServerMessage(MessageSeverity.Information, "Welcome to Absolute Roleplay Sync, Current Online Users: " + _systemInfoService.SystemInfoDto.OnlineUsers).ConfigureAwait(false);

        return new ConnectionDto(new UserData(dbUser.UID, string.IsNullOrWhiteSpace(dbUser.Alias) ? null : dbUser.Alias))
        {
            CurrentClientVersion = _expectedClientVersion,
            ServerVersion = IMareHub.ApiVersion,
            IsAdmin = dbUser.IsAdmin,
            IsModerator = dbUser.IsModerator,
            ServerInfo = new ServerInfo()
            {
                MaxGroupsCreatedByUser = _maxExistingGroupsByUser,
                ShardName = _shardName,
                MaxGroupsJoinedByUser = _maxJoinedGroupsByUser,
                MaxGroupUserCount = _maxGroupUserCount,
                FileServerAddress = _fileServerAddress,
                MaxCharaData = _maxCharaDataByUser
            },
        };
    }

    [Authorize(Policy = "Authenticated")]
    public async Task<bool> CheckClientHealth()
    {
        await UpdateUserOnRedis().ConfigureAwait(false);

        return false;
    }

    [Authorize(Policy = "Authenticated")]
    public override async Task OnConnectedAsync()
    {
        _mareMetrics.IncGaugeWithLabels(MetricsAPI.GaugeConnections, labels: Continent);

        try
        {
            _logger.LogCallInfo(MareHubLogger.Args(_contextAccessor.GetIpAddress(), UserCharaIdent));

            await UpdateUserOnRedis().ConfigureAwait(false);
        }
        catch { }

        await base.OnConnectedAsync().ConfigureAwait(false);
    }

    [Authorize(Policy = "Authenticated")]
    public override async Task OnDisconnectedAsync(Exception exception)
    {
        _mareMetrics.DecGaugeWithLabels(MetricsAPI.GaugeConnections, labels: Continent);

        try
        {
            _logger.LogCallInfo(MareHubLogger.Args(_contextAccessor.GetIpAddress(), UserCharaIdent));
            if (exception != null)
                _logger.LogCallWarning(MareHubLogger.Args(_contextAccessor.GetIpAddress(), exception.Message, exception.StackTrace));

            await GposeLobbyLeave().ConfigureAwait(false);
            await RemoveUserFromRedis().ConfigureAwait(false);

            await SendOfflineToAllPairedUsers().ConfigureAwait(false);
        }
        catch { }

        await base.OnDisconnectedAsync(exception).ConfigureAwait(false);
    }
}

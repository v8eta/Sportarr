using Sportarr.Api.Data;
using Sportarr.Api.Models;
using Microsoft.EntityFrameworkCore;

namespace Sportarr.Api.Services;

/// <summary>
/// Enhanced background service that monitors download clients with comprehensive features:
/// - Download progress tracking
/// - Completed download handling and auto-import
/// - Failed download detection and auto-retry
/// - Stalled download detection
/// - Blocklist management
/// - Remove completed downloads option
/// </summary>
public class EnhancedDownloadMonitorService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<EnhancedDownloadMonitorService> _logger;
    private readonly TimeSpan _pollInterval = TimeSpan.FromSeconds(30);

    // Hard cap on import retries. After this many failed import attempts the
    // row is marked Failed permanently and the monitor stops touching it —
    // otherwise the download client (e.g. SABnzbd) will keep reporting the
    // item as 100% complete on every poll, the monitor will keep flipping
    // Failed→Completed, and HandleCompletedDownload will keep retrying the
    // same broken import forever. Without this cap we've seen ImportRetryCount
    // climb past 1000 in production. (Mirrors the same constant in
    // DownloadProcessingService, which owns the actual per-item retry logic —
    // this copy just gates which rows are even pulled into this poll.)
    private const int MaxImportRetries = 3;

    public EnhancedDownloadMonitorService(
        IServiceProvider serviceProvider,
        ILogger<EnhancedDownloadMonitorService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("[Enhanced Download Monitor] Service started - Poll interval: {Interval}s", _pollInterval.TotalSeconds);

        // Wait before starting to allow app to fully initialize
        await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

        // Reset MissingFromClientCount for all active downloads on startup.
        // This prevents stale counts from a previous shutdown from causing false "removed externally" removals.
        // Counts are only meaningful within a single continuous run.
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<SportarrDbContext>();
            var activeDownloads = await db.DownloadQueue
                .Where(d => d.MissingFromClientCount > 0 &&
                            d.Status != DownloadStatus.Imported &&
                            d.Status != DownloadStatus.Failed)
                .ToListAsync(stoppingToken);

            if (activeDownloads.Count > 0)
            {
                _logger.LogInformation("[Enhanced Download Monitor] Resetting MissingFromClientCount for {Total} download(s) on startup (prevents false removal after restart)",
                    activeDownloads.Count);
                foreach (var d in activeDownloads)
                {
                    _logger.LogInformation("[Enhanced Download Monitor] Resetting MissingFromClientCount={Count} for '{Title}' on startup",
                        d.MissingFromClientCount, d.Title);
                    d.MissingFromClientCount = 0;
                }
                await db.SaveChangesAsync(stoppingToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Enhanced Download Monitor] Failed to reset MissingFromClientCount on startup");
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await MonitorDownloadsAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Enhanced Download Monitor] Error monitoring downloads");
            }

            // Detect external downloads (added to client outside of Sportarr)
            try
            {
                await DetectExternalDownloadsAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Enhanced Download Monitor] Error detecting external downloads");
            }

            await Task.Delay(_pollInterval, stoppingToken);
        }

        _logger.LogInformation("[Enhanced Download Monitor] Service stopped");
    }

    private async Task MonitorDownloadsAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<SportarrDbContext>();
        var downloadClientService = scope.ServiceProvider.GetRequiredService<DownloadClientService>();
        var fileImportService = scope.ServiceProvider.GetRequiredService<FileImportService>();
        var configService = scope.ServiceProvider.GetRequiredService<ConfigService>();
        var downloadProcessingService = scope.ServiceProvider.GetRequiredService<DownloadProcessingService>();

        // Hygiene: drop queue and grab-history rows whose event no longer exists.
        // These orphans appear when an event is removed (its DownloadQueue cascade
        // isn't enforced on every legacy DB), leaving stale "Completed" rows that
        // clutter the Activity queue and that the dedup (keyed on event id) can't
        // match — so the same release looks un-grabbed and gets re-grabbed. Cheap
        // anti-join, normally deletes nothing once events are stable.
        try
        {
            var removedQueue = await db.DownloadQueue
                .Where(d => !db.Events.Any(e => e.Id == d.EventId))
                .ExecuteDeleteAsync(cancellationToken);
            var removedGrabs = await db.GrabHistory
                .Where(g => !db.Events.Any(e => e.Id == g.EventId))
                .ExecuteDeleteAsync(cancellationToken);
            if (removedQueue > 0 || removedGrabs > 0)
            {
                _logger.LogInformation(
                    "[Enhanced Download Monitor] Cleaned up orphaned rows whose event no longer exists: {Queue} queue, {Grabs} grab-history",
                    removedQueue, removedGrabs);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Enhanced Download Monitor] Orphaned-row cleanup failed");
        }

        // Get all active downloads (not completed, not imported, not failed permanently).
        //
        // Two separate retry counters gate exclusion:
        //   - RetryCount         : incremented when the *download* itself fails
        //                          (HandleFailedDownload). Re-grab is allowed up to 3 attempts.
        //   - ImportRetryCount   : incremented when the *import* of a completed
        //                          download fails. After MaxImportRetries the row is
        //                          permanently Failed and we must NOT pick it up again,
        //                          even though SAB will still happily report it as
        //                          100% complete on every poll.
        //
        // Without the ImportRetryCount gate, the previous query kept pulling Failed
        // rows whose RetryCount was 0 (download succeeded, only import broke), the
        // monitor flipped Failed→Completed because the client said "completed", and
        // HandleCompletedDownload retried the same broken import forever.
        var activeDownloads = await db.DownloadQueue
            .Include(d => d.DownloadClient)
            .Include(d => d.Event)
            .Where(d => d.Status != DownloadStatus.Imported &&
                       (d.Status != DownloadStatus.Failed
                            || (d.RetryCount < 3 && (d.ImportRetryCount ?? 0) < MaxImportRetries)))
            .ToListAsync(cancellationToken);

        if (activeDownloads.Count == 0)
            return;

        _logger.LogDebug("[Enhanced Download Monitor] Checking {Count} active downloads", activeDownloads.Count);

        // Load settings once
        var config = await configService.GetConfigAsync();
        var enableCompletedHandling = config.EnableCompletedDownloadHandling;
        var redownloadFailed = config.RedownloadFailedDownloads;
        var redownloadFailedFromInteractive = config.RedownloadFailedFromInteractiveSearch;
        // Note: RemoveCompletedDownloads and RemoveFailedDownloads are now per-client settings
        // accessed via download.DownloadClient.RemoveCompletedDownloads/RemoveFailedDownloads

        foreach (var download in activeDownloads)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            try
            {
                await downloadProcessingService.ProcessDownloadAsync(
                    download,
                    downloadClientService,
                    fileImportService,
                    db,
                    enableCompletedHandling,
                    redownloadFailed,
                    redownloadFailedFromInteractive,
                    cancellationToken);

                // Save changes after each successful download to prevent data loss
                await db.SaveChangesAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Enhanced Download Monitor] Error processing download: {Title}", download.Title);

                // Mark as failed but allow retry
                download.Status = DownloadStatus.Failed;
                download.ErrorMessage = ex.Message;
                download.RetryCount = (download.RetryCount ?? 0) + 1;

                // Save the error state immediately
                try
                {
                    await db.SaveChangesAsync(cancellationToken);
                }
                catch (Exception saveEx)
                {
                    _logger.LogError(saveEx, "[Enhanced Download Monitor] Failed to save error state for download: {Title}", download.Title);
                }
            }
        }
    }

    /// <summary>
    /// Detect completed downloads in download clients that were added externally (not through Sportarr).
    /// Creates PendingImport records so users can review and accept/reject them in the Activity page.
    /// </summary>
    private async Task DetectExternalDownloadsAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<SportarrDbContext>();
        var downloadClientService = scope.ServiceProvider.GetRequiredService<DownloadClientService>();

        // Get all enabled download clients
        var clients = await db.DownloadClients
            .Where(c => c.Enabled)
            .ToListAsync(cancellationToken);

        if (clients.Count == 0) return;

        // Get all known download IDs to filter out:
        // 1. Active downloads in queue (Sportarr-initiated, currently downloading/importing)
        // CASE-INSENSITIVE comparer: qBittorrent/SABnzbd can return the torrent hash or nzb id
        // in a different case between the initial add response and later /info polls. Without
        // OrdinalIgnoreCase the HashSet would miss the match and Sportarr-grabbed downloads
        // would re-appear as "external" PendingImport rows.
        var knownDownloadIds = new HashSet<string>(
            await db.DownloadQueue.Select(d => d.DownloadId).ToListAsync(cancellationToken),
            StringComparer.OrdinalIgnoreCase);

        // 2. ALL pending imports (any status — prevents re-detection of completed/rejected imports)
        var pendingDownloadIds = new HashSet<string>(
            await db.PendingImports
                .Select(pi => pi.DownloadId)
                .ToListAsync(cancellationToken),
            StringComparer.OrdinalIgnoreCase);

        // 3. Grab history (Sportarr-initiated downloads that have been imported and removed from queue)
        var grabbedDownloadIds = new HashSet<string>(
            await db.GrabHistory
                .Where(g => g.DownloadId != null)
                .Select(g => g.DownloadId!)
                .Distinct()
                .ToListAsync(cancellationToken),
            StringComparer.OrdinalIgnoreCase);

        // Hash-based fallback dedup. Real-Debrid uncached downloads can return
        // a different DownloadId from Decypharr at grab-time vs poll-time (the
        // ID changes once RD finishes caching the torrent), so DownloadId alone
        // misses the duplicate. The torrent info hash stays stable.
        var knownHashes = new HashSet<string>(
            (await db.DownloadQueue
                .Where(d => d.TorrentInfoHash != null)
                .Select(d => d.TorrentInfoHash!)
                .Concat(db.PendingImports
                    .Where(pi => pi.TorrentInfoHash != null)
                    .Select(pi => pi.TorrentInfoHash!))
                .Concat(db.GrabHistory
                    .Where(g => g.TorrentInfoHash != null)
                    .Select(g => g.TorrentInfoHash!))
                .ToListAsync(cancellationToken))
            .Select(h => h.ToLowerInvariant()),
            StringComparer.OrdinalIgnoreCase);

        // Blocklist dedup. When the user clicks Remove on a
        // pending import, the row is hard-deleted and a Blocklist entry is
        // written. If the download client silently fails to actually delete
        // the download (SABnzbd's queue-delete returns success even for
        // history-only ids; some torrent clients keep completed torrents in
        // a history view), the next poll would otherwise re-detect it as a
        // brand-new external download and recreate the PendingImport row,
        // producing the infinite re-add loop the user reported.
        var blocklistedHashes = new HashSet<string>(
            (await db.Blocklist
                .Where(b => b.TorrentInfoHash != null)
                .Select(b => b.TorrentInfoHash!)
                .ToListAsync(cancellationToken))
            .Select(h => h.ToLowerInvariant()),
            StringComparer.OrdinalIgnoreCase);
        var blocklistedTitles = new HashSet<string>(
            await db.Blocklist
                .Select(b => b.Title)
                .ToListAsync(cancellationToken),
            StringComparer.OrdinalIgnoreCase);

        foreach (var client in clients)
        {
            if (cancellationToken.IsCancellationRequested) break;

            try
            {
                var allDownloads = await downloadClientService.GetAllDownloadsByCategoryAsync(client, client.Category);

                foreach (var download in allDownloads)
                {
                    // Skip downloads we already know about (queue, pending imports,
                    // or grab history). Match by DownloadId first, then by torrent
                    // hash as a fallback for Real-Debrid uncached downloads where
                    // Decypharr returns a different DownloadId at grab vs poll time.
                    if (knownDownloadIds.Contains(download.DownloadId))
                    {
                        _logger.LogDebug("[Enhanced Download Monitor] Skipping '{Title}' (id {Id}) — active in DownloadQueue",
                            download.Title, download.DownloadId);
                        continue;
                    }
                    if (pendingDownloadIds.Contains(download.DownloadId))
                    {
                        _logger.LogDebug("[Enhanced Download Monitor] Skipping '{Title}' (id {Id}) — already a PendingImport awaiting user resolution",
                            download.Title, download.DownloadId);
                        continue;
                    }
                    if (grabbedDownloadIds.Contains(download.DownloadId))
                    {
                        _logger.LogDebug("[Enhanced Download Monitor] Skipping '{Title}' (id {Id}) — Sportarr-grabbed (in GrabHistory)",
                            download.Title, download.DownloadId);
                        continue;
                    }

                    if (!string.IsNullOrEmpty(download.TorrentInfoHash) &&
                        knownHashes.Contains(download.TorrentInfoHash))
                    {
                        _logger.LogDebug(
                            "[Enhanced Download Monitor] Skipping '{Title}' — hash {Hash} already tracked under a different DownloadId (Real-Debrid id mutation case)",
                            download.Title, download.TorrentInfoHash);
                        continue;
                    }

                    if (!string.IsNullOrEmpty(download.TorrentInfoHash) &&
                        blocklistedHashes.Contains(download.TorrentInfoHash))
                    {
                        _logger.LogDebug(
                            "[Enhanced Download Monitor] Skipping '{Title}' — hash {Hash} is blocklisted (user previously rejected)",
                            download.Title, download.TorrentInfoHash);
                        continue;
                    }
                    if (blocklistedTitles.Contains(download.Title))
                    {
                        _logger.LogDebug(
                            "[Enhanced Download Monitor] Skipping '{Title}' — title is blocklisted (user previously rejected)",
                            download.Title);
                        continue;
                    }

                    // Try to match to an event by title
                    int? suggestedEventId = null;
                    int confidence = 0;

                    // Simple title matching: search for events whose title contains key words from download title
                    var cleanTitle = CleanDownloadTitle(download.Title);
                    if (!string.IsNullOrEmpty(cleanTitle))
                    {
                        var pattern = $"%{cleanTitle}%";
                        var matchedEvent = await db.Events
                            .Where(e => !e.HasFile)
                            .Where(e => EF.Functions.Like(e.Title, pattern) ||
                                       e.Title != null && cleanTitle.Contains(e.Title))
                            .FirstOrDefaultAsync(cancellationToken);

                        if (matchedEvent != null)
                        {
                            suggestedEventId = matchedEvent.Id;
                            confidence = 50; // Basic title match
                        }
                    }

                    // Create pending import
                    var pendingImport = new PendingImport
                    {
                        DownloadClientId = client.Id,
                        DownloadId = download.DownloadId,
                        Title = download.Title,
                        FilePath = download.FilePath,
                        Size = download.Size,
                        Protocol = download.Protocol,
                        TorrentInfoHash = download.TorrentInfoHash,
                        SuggestedEventId = suggestedEventId,
                        SuggestionConfidence = confidence,
                        Detected = DateTime.UtcNow,
                        Status = PendingImportStatus.Pending
                    };

                    db.PendingImports.Add(pendingImport);
                    pendingDownloadIds.Add(download.DownloadId); // Prevent duplicates within this scan
                    if (!string.IsNullOrEmpty(download.TorrentInfoHash))
                        knownHashes.Add(download.TorrentInfoHash);

                    _logger.LogInformation(
                        "[Enhanced Download Monitor] Detected external download: {Title} (Client: {Client}, Id: {Id}, Hash: {Hash}, Confidence: {Confidence}%) — no match in DownloadQueue, PendingImports, GrabHistory, knownHashes, or Blocklist",
                        download.Title, client.Name, download.DownloadId,
                        download.TorrentInfoHash ?? "(none)", confidence);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "[Enhanced Download Monitor] Error checking external downloads for client: {Client}", client.Name);
            }
        }

        await db.SaveChangesAsync(cancellationToken);
    }

    /// <summary>
    /// Clean a download title for basic matching by removing quality tags, dots, etc.
    /// </summary>
    private static string CleanDownloadTitle(string title)
    {
        // Remove common quality/source tags
        var cleaned = System.Text.RegularExpressions.Regex.Replace(title,
            @"[\.\-_](1080p|720p|2160p|4K|WEB-DL|WEBRip|BluRay|HDTV|x264|x265|HEVC|AAC|DDP?\d?\.\d|AMZN|NF|HULU).*$",
            "", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        // Replace dots and underscores with spaces
        cleaned = cleaned.Replace('.', ' ').Replace('_', ' ').Replace('-', ' ');

        return cleaned.Trim();
    }
}

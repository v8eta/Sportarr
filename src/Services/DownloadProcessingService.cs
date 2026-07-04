using Sportarr.Api.Data;
using Sportarr.Api.Models;
using Microsoft.EntityFrameworkCore;

namespace Sportarr.Api.Services;

/// <summary>
/// Single-item download status check + import logic. Extracted from
/// EnhancedDownloadMonitorService so the same code path can be invoked two ways:
///   1. The 30s poller (EnhancedDownloadMonitorService), as a backstop.
///   2. The on-demand completion webhook (POST /api/v3/download/completed) that
///      qBittorrent/SABnzbd call the instant a download finishes, instead of
///      waiting for the next poll tick. This closes the race window where a
///      near-simultaneous, lower-scoring release for the same event can slip
///      past the "already have a better file" check because the just-accepted
///      grab hasn't been imported into the DB yet.
/// </summary>
public class DownloadProcessingService
{
    private readonly ILogger<DownloadProcessingService> _logger;
    private readonly TimeSpan _stalledTimeout = TimeSpan.FromMinutes(10);

    // Hard cap on import retries. After this many failed import attempts the
    // row is marked Failed permanently and the monitor stops touching it —
    // otherwise the download client (e.g. SABnzbd) will keep reporting the
    // item as 100% complete on every poll, the monitor will keep flipping
    // Failed→Completed, and HandleCompletedDownload will keep retrying the
    // same broken import forever. Without this cap we've seen ImportRetryCount
    // climb past 1000 in production.
    private const int MaxImportRetries = 3;

    public DownloadProcessingService(ILogger<DownloadProcessingService> logger)
    {
        _logger = logger;
    }

    public async Task ProcessDownloadAsync(
        DownloadQueueItem download,
        DownloadClientService downloadClientService,
        FileImportService fileImportService,
        SportarrDbContext db,
        bool enableCompletedHandling,
        bool redownloadFailed,
        bool redownloadFailedFromInteractive,
        CancellationToken cancellationToken)
    {
        // For ImportPending downloads, skip the download client check and just retry import
        // The download already completed on the client, we're just waiting for the file to be accessible
        if (download.Status == DownloadStatus.ImportPending && enableCompletedHandling)
        {
            _logger.LogDebug("[Download Processing] Retrying import for pending download: {Title} (attempt {Count})",
                download.Title, (download.ImportRetryCount ?? 0) + 1);

            // Re-check the download client first. The data on disk may
            // have disappeared between the time we last saw "complete"
            // and now (qbit's missingFiles state for torrents whose
            // content was moved/deleted; SAB removing the history
            // entry; debrid orphan-cleanup running). Without this
            // check we waste 3 import retries (~2 minutes) per cycle
            // attempting to read a path the client has already given
            // up on. If the client now reports the download as failed
            // or no longer present, route to HandleFailedDownload
            // immediately so the Blocklist entry, qbit removal, and
            // re-search trigger fire on the FIRST poll instead of the
            // 4th.
            if (download.DownloadClient != null && !string.IsNullOrEmpty(download.DownloadId))
            {
                try
                {
                    var clientStatus = await downloadClientService.GetDownloadStatusAsync(
                        download.DownloadClient,
                        download.DownloadId);

                    var clientReportsFailure = clientStatus != null &&
                        string.Equals(clientStatus.Status, "failed", StringComparison.OrdinalIgnoreCase);

                    if (clientReportsFailure)
                    {
                        _logger.LogWarning(
                            "[Download Processing] Download client now reports {Title} as failed ({Reason}); short-circuiting import retry loop.",
                            download.Title, clientStatus!.ErrorMessage ?? "no detail");
                        download.Status = DownloadStatus.Failed;
                        download.ErrorMessage = clientStatus.ErrorMessage ?? "Download client reports data missing";
                        await HandleFailedDownload(
                            download,
                            downloadClientService,
                            db,
                            redownloadFailed,
                            redownloadFailedFromInteractive);
                        return;
                    }
                }
                catch (Exception ex)
                {
                    // Client query failed - log but fall through to
                    // the import retry. We don't want a transient
                    // download-client outage to mark every pending
                    // import as failed.
                    _logger.LogDebug(ex,
                        "[Download Processing] Could not re-query download client for {Title} during import retry; proceeding with retry anyway",
                        download.Title);
                }
            }

            // Capture the status before the import retry so we can
            // detect the same Failed transition the long path
            // handles below. Without this, when attempt N/N flips
            // status to Failed inside HandleCompletedDownload, the
            // early `return` on the next line skips the
            // HandleFailedDownload call at line 414 and the
            // download silently rots in Failed state with no
            // blocklist entry, no re-search, and no notification.
            var previousImportPendingStatus = download.Status;
            await HandleCompletedDownload(
                download,
                downloadClientService,
                fileImportService,
                db);

            if (download.Status == DownloadStatus.Failed
                && previousImportPendingStatus != DownloadStatus.Failed)
            {
                await HandleFailedDownload(
                    download,
                    downloadClientService,
                    db,
                    redownloadFailed,
                    redownloadFailedFromInteractive);
            }
            return;
        }

        if (download.DownloadClient == null)
        {
            _logger.LogWarning("[Download Processing] Download {Title} has no download client assigned", download.Title);
            download.Status = DownloadStatus.Failed;
            download.ErrorMessage = "No download client assigned";
            return;
        }

        // Query download client for current status
        var status = await downloadClientService.GetDownloadStatusAsync(
            download.DownloadClient,
            download.DownloadId);

        if (status == null)
        {
            // Download not found by ID - try finding by title (Decypharr/debrid proxy compatibility)
            // Debrid proxies may change the download ID/hash after processing
            _logger.LogInformation("[Download Processing] Download not found by ID {DownloadId}, trying title match for: {Title} (MissingCount so far: {Count})",
                download.DownloadId, download.Title, download.MissingFromClientCount ?? 0);

            var (titleMatchStatus, newDownloadId) = await downloadClientService.FindDownloadByTitleAsync(
                download.DownloadClient,
                download.Title,
                download.DownloadClient.Category);

            if (titleMatchStatus != null && newDownloadId != null)
            {
                _logger.LogInformation("[Download Processing] Found download by title match. Updating ID: {OldId} → {NewId}",
                    download.DownloadId, newDownloadId);

                // Update the download ID to the new one (debrid proxy changed it)
                download.DownloadId = newDownloadId;
                status = titleMatchStatus;
            }
            else
            {
                // Download not found in client: auto-remove from queue.
                // This happens when user deletes from download client directly instead of through Sportarr.

                // Do NOT count this as "missing" if we're shutting down — the null could be from a cancelled HTTP request
                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("[Download Processing] Download status check cancelled for '{Title}' - skipping missing count increment", download.Title);
                    return;
                }

                // Grace period: newly added downloads may not be visible in client yet
                // Transmission/other clients can take several minutes to register a torrent
                var gracePeriod = TimeSpan.FromMinutes(3);
                if (download.Added > DateTime.UtcNow - gracePeriod)
                {
                    _logger.LogDebug("[Download Processing] Download recently added ({Age:F0}s ago), skipping missing check during grace period: {Title}",
                        (DateTime.UtcNow - download.Added).TotalSeconds, download.Title);
                    return;
                }

                // Track consecutive "not found" checks to avoid removing on transient issues
                download.MissingFromClientCount = (download.MissingFromClientCount ?? 0) + 1;

                if (download.MissingFromClientCount >= 10)
                {
                    // After 10 consecutive checks (e.g. ~5 minutes at 30s poll interval), remove from queue.
                    // Downloads removed from the client are removed from the queue.
                    _logger.LogWarning("[Download Processing] Download not found in client for {Count} consecutive checks, removing from queue: {Title} (DownloadId: {DownloadId})",
                        download.MissingFromClientCount, download.Title, download.DownloadId);

                    // Remove from queue (auto-cleanup).
                    db.DownloadQueue.Remove(download);
                    await db.SaveChangesAsync();
                    return;
                }
                else
                {
                    // First few "not found" checks — log at Warning so they are visible in production
                    _logger.LogWarning("[Download Processing] Download not found in client (check {Count}/10): {Title} (DownloadId: {DownloadId})",
                        download.MissingFromClientCount, download.Title, download.DownloadId);
                }
                return;
            }
        }

        // Download found - reset "missing from client" counter
        download.MissingFromClientCount = 0;

        // Update download metadata
        var previousStatus = download.Status;
        var previousProgress = download.Progress;

        download.Progress = status.Progress;
        download.Downloaded = status.Downloaded;
        download.Size = status.Size;
        download.TimeRemaining = status.TimeRemaining;
        download.LastUpdate = DateTime.UtcNow;

        // Update status based on client response
        // Special handling for Decypharr: "paused" with 100% progress means completed
        // Decypharr pauses torrents when complete since debrid services don't seed
        var isDecypharrCompleted = status.Status == "paused" && status.Progress >= 99.9;

        download.Status = status.Status switch
        {
            "downloading" => DownloadStatus.Downloading,
            "paused" when isDecypharrCompleted => DownloadStatus.Completed,
            "paused" => DownloadStatus.Paused,
            "completed" => DownloadStatus.Completed,
            "failed" or "error" => DownloadStatus.Failed,
            "queued" or "waiting" => DownloadStatus.Queued,
            "warning" => DownloadStatus.Warning,
            _ => download.Status
        };

        if (isDecypharrCompleted)
        {
            _logger.LogInformation("[Download Processing] Detected Decypharr-style completion (paused at 100%): {Title}", download.Title);
        }

        if (!string.IsNullOrEmpty(status.ErrorMessage))
        {
            download.ErrorMessage = status.ErrorMessage;
        }

        // Log status changes
        if (previousStatus != download.Status)
        {
            _logger.LogInformation("[Download Processing] '{Title}' status: {Old} → {New} ({Progress:F1}%)",
                download.Title, previousStatus, download.Status, download.Progress);
        }

        // Warn if the event is no longer monitored.
        // This applies when user unmonitors an event/league/season while download is in progress.
        if (download.Event != null && !download.Event.Monitored)
        {
            // Only set warning status if not already completed/imported/failed
            // AND only if this was NOT a manual grab — manual grabs should always import
            // regardless of event monitoring status (matches AutomaticSearchService behavior)
            if (download.Status != DownloadStatus.Imported &&
                download.Status != DownloadStatus.Failed &&
                !download.IsManualSearch)
            {
                download.Status = DownloadStatus.Warning;

                // Add unmonitored warning to StatusMessages if not already present
                var unmonitoredMessage = "Event is no longer monitored";
                if (!download.StatusMessages.Contains(unmonitoredMessage))
                {
                    download.StatusMessages.Add(unmonitoredMessage);
                    _logger.LogWarning("[Download Processing] '{Title}' - Event is no longer monitored, download marked as warning",
                        download.Title);
                }
            }
        }
        else
        {
            // Remove unmonitored warning if event is now monitored again
            var unmonitoredMessage = "Event is no longer monitored";
            if (download.StatusMessages.Contains(unmonitoredMessage))
            {
                download.StatusMessages.Remove(unmonitoredMessage);
                _logger.LogInformation("[Download Processing] '{Title}' - Event is now monitored again, warning removed",
                    download.Title);

                // Reset status to previous state if the only warning was unmonitored
                if (download.StatusMessages.Count == 0 && download.Status == DownloadStatus.Warning)
                {
                    download.Status = status.Status switch
                    {
                        "downloading" => DownloadStatus.Downloading,
                        "paused" => DownloadStatus.Paused,
                        "completed" => DownloadStatus.Completed,
                        "queued" or "waiting" => DownloadStatus.Queued,
                        _ => DownloadStatus.Downloading
                    };
                }
            }
        }

        // Detect stalled downloads
        if (download.Status == DownloadStatus.Downloading)
        {
            CheckForStalledDownload(download, previousProgress, db);
        }

        // Handle completed downloads
        // Import if: (1) status just changed to Completed, OR (2) already Completed but not yet imported
        // The second case handles downloads that arrive already completed (common with debrid services)
        if (download.Status == DownloadStatus.Completed &&
            download.Status != DownloadStatus.Imported &&
            (previousStatus != DownloadStatus.Completed || download.ImportedAt == null) &&
            enableCompletedHandling)
        {
            await HandleCompletedDownload(
                download,
                downloadClientService,
                fileImportService,
                db);
        }

        // Always handle failed downloads (no global disable — Radarr parity)
        if (download.Status == DownloadStatus.Failed &&
            previousStatus != DownloadStatus.Failed)
        {
            await HandleFailedDownload(
                download,
                downloadClientService,
                db,
                redownloadFailed,
                redownloadFailedFromInteractive);
        }
    }

    private void CheckForStalledDownload(
        DownloadQueueItem download,
        double previousProgress,
        SportarrDbContext db)
    {
        // If progress hasn't changed and we've been downloading for a while
        if (Math.Abs(download.Progress - previousProgress) < 0.1 && download.Added < DateTime.UtcNow - _stalledTimeout)
        {
            // Check if this is the first time we've detected stalled state
            if (!download.ErrorMessage?.Contains("stalled") == true)
            {
                _logger.LogWarning("[Download Processing] Download appears stalled: {Title} (Progress: {Progress:F1}%)",
                    download.Title, download.Progress);

                download.Status = DownloadStatus.Warning;
                download.ErrorMessage = $"Download stalled at {download.Progress:F1}% for {_stalledTimeout.TotalMinutes} minutes";
            }
        }
    }

    /// <summary>
    /// Check if a torrent has reached its seed limits (ratio and/or time) from the indexer settings.
    /// Returns true if all configured limits are met, or if no limits are configured.
    /// </summary>
    private static bool HasReachedSeedLimit(DownloadClientStatus status, Indexer indexer)
    {
        // Check ratio limit
        if (indexer.SeedRatio.HasValue && indexer.SeedRatio.Value > 0)
        {
            if ((status.Ratio ?? 0) < indexer.SeedRatio.Value)
                return false;
        }

        // Check time limit (SeedTime is in minutes)
        if (indexer.SeedTime.HasValue && indexer.SeedTime.Value > 0)
        {
            var seedingMinutes = status.CompletedAt.HasValue
                ? (DateTime.UtcNow - status.CompletedAt.Value).TotalMinutes
                : 0;

            if (seedingMinutes < indexer.SeedTime.Value)
                return false;
        }

        return true;
    }

    private async Task HandleCompletedDownload(
        DownloadQueueItem download,
        DownloadClientService downloadClientService,
        FileImportService fileImportService,
        SportarrDbContext? db = null)
    {
        download.CompletedAt = DateTime.UtcNow;

        // Defensive guard: even though the caller filters out rows with
        // ImportRetryCount >= MaxImportRetries, the status flip from
        // Failed→Completed earlier in this method's call stack can let a row
        // reach here that has already exhausted its retries. Don't burn another
        // attempt on it — pin it to Failed and walk away.
        if ((download.ImportRetryCount ?? 0) >= MaxImportRetries)
        {
            download.Status = DownloadStatus.Failed;
            if (string.IsNullOrEmpty(download.ErrorMessage))
            {
                download.ErrorMessage = $"Import failed after {MaxImportRetries} attempts; not retrying";
            }
            return;
        }

        _logger.LogInformation("[Download Processing] Download completed, starting import: {Title}", download.Title);

        try
        {
            download.Status = DownloadStatus.Importing;

            // Import the download
            await fileImportService.ImportDownloadAsync(download);

            download.Status = DownloadStatus.Imported;
            download.ImportedAt = DateTime.UtcNow;

            _logger.LogInformation("[Download Processing] ✓ Import successful: {Title}", download.Title);

            // Remove from download client if configured in the client's settings
            // Pass deleteFiles: true to also remove the download folder from disk
            // The video files have already been moved/hardlinked to the library, but non-video files (nfo, srr, etc.)
            // and the folder itself may remain - the download client should clean these up
            //
            // Uses per-client RemoveCompletedDownloads setting which allows users to configure
            // differently for each client (e.g., remove for Usenet, preserve for seeding torrents)
            if (download.DownloadClient?.RemoveCompletedDownloads == true)
            {
                // For torrents with indexer seed settings, check if seeding goals are met before removal.
                // Torrents seed until ratio/time limits are reached.
                if (download.Protocol == "Torrent" && db != null)
                {
                    var indexer = download.IndexerId != null
                        ? await db.Indexers.FindAsync(download.IndexerId)
                        : !string.IsNullOrEmpty(download.Indexer)
                            ? await db.Indexers.FirstOrDefaultAsync(i => i.Name == download.Indexer)
                            : null;

                    if (indexer != null && (indexer.SeedRatio.HasValue || indexer.SeedTime.HasValue))
                    {
                        var status = await downloadClientService.GetDownloadStatusAsync(
                            download.DownloadClient, download.DownloadId);

                        if (status != null && !HasReachedSeedLimit(status, indexer))
                        {
                            _logger.LogInformation(
                                "[Download Processing] Torrent still seeding, skipping removal: {Title} " +
                                "(Ratio: {Ratio:F2}/{Target}, Time: {Time})",
                                download.Title,
                                status.Ratio ?? 0,
                                indexer.SeedRatio?.ToString("F1") ?? "N/A",
                                indexer.SeedTime.HasValue ? $"{indexer.SeedTime}min" : "N/A");

                            // Mark as imported but don't remove — monitor will re-check on next poll
                            return;
                        }
                    }
                }

                try
                {
                    await downloadClientService.RemoveDownloadAsync(
                        download.DownloadClient,
                        download.DownloadId,
                        deleteFiles: true);

                    _logger.LogDebug("[Download Processing] Removed completed download from client: {Title}", download.Title);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[Download Processing] Failed to remove download from client: {Title}", download.Title);
                    // Don't fail the import if we can't remove from client
                }
            }
            else if (download.DownloadClient == null)
            {
                // Log when download client removal is skipped due to missing client association
                // This helps diagnose why folders might not be removed from the download client
                _logger.LogDebug("[Download Processing] Skipped removal from download client: No download client associated with {Title}",
                    download.Title);
            }
        }
        catch (IndexerFailDownloadException ex)
        {
            // FailDownloads policy match. Skip the retry-count loop —
            // pin to Failed so the next monitor pass takes the
            // status-transition path in HandleFailedDownload, which
            // adds to the blocklist and (if redownloadFailed is on)
            // schedules a re-search. Bumping ImportRetryCount here
            // would burn the retry budget on a release that's never
            // going to import successfully.
            _logger.LogWarning(
                "[Download Processing] ✗ FailDownloads policy fired ({Reason}) for {Title}: {Message}",
                ex.Reason, download.Title, ex.Message);
            download.Status = DownloadStatus.Failed;
            download.ErrorMessage = ex.Message;
        }
        catch (DownloadFailedException ex)
        {
            // The download client itself flagged this as failed (e.g.
            // SAB renamed the folder _FAILED_<x> after a par2/unpack
            // post-processing failure). Same routing as the
            // FailDownloads policy match: skip retries and pin to
            // Failed so HandleFailedDownload's status-transition path
            // blocklists the release and schedules a re-search. Without
            // this branch the import would re-attempt 3× into an empty
            // folder, never blocklist, and the next RSS sync would
            // re-grab the same broken NZB indefinitely.
            _logger.LogWarning(
                "[Download Processing] ✗ Download client reported failure for {Title}: {Message}",
                download.Title, ex.Message);
            download.Status = DownloadStatus.Failed;
            download.ErrorMessage = ex.Message;
        }
        catch (Exception ex)
        {
            download.ImportRetryCount = (download.ImportRetryCount ?? 0) + 1;

            // Check if this is a path accessibility issue (file not ready yet)
            var isPathError = ex.Message.Contains("not found") ||
                             ex.Message.Contains("not accessible") ||
                             ex.Message.Contains("does not exist");

            if (isPathError)
            {
                // For path accessibility issues, keep retrying indefinitely
                // The file might just be delayed (still extracting, moving, etc.)
                _logger.LogWarning("[Download Processing] Import path not accessible (attempt {Count}): {Title} - Will retry on next poll",
                    download.ImportRetryCount, download.Title);

                download.Status = DownloadStatus.ImportPending;
                download.ErrorMessage = $"Waiting for path to be accessible (attempt {download.ImportRetryCount}): {ex.Message}";
            }
            else
            {
                // For other import errors, treat as failed after MaxImportRetries attempts.
                _logger.LogError(ex, "[Download Processing] ✗ Import failed (attempt {Count}/{Max}): {Title}",
                    download.ImportRetryCount, MaxImportRetries, download.Title);

                if (download.ImportRetryCount >= MaxImportRetries)
                {
                    download.Status = DownloadStatus.Failed;
                    download.ErrorMessage = $"Import failed after {MaxImportRetries} attempts: {ex.Message}";
                }
                else
                {
                    download.Status = DownloadStatus.ImportPending;
                    download.ErrorMessage = $"Import failed (attempt {download.ImportRetryCount}/{MaxImportRetries}): {ex.Message}";
                }
            }
        }
    }

    private async Task HandleFailedDownload(
        DownloadQueueItem download,
        DownloadClientService downloadClientService,
        SportarrDbContext db,
        bool redownloadFailed,
        bool redownloadFailedFromInteractive)
    {
        download.RetryCount = (download.RetryCount ?? 0) + 1;

        _logger.LogWarning("[Download Processing] Download failed: {Title} (Attempt {Retry}/3) - {Error}",
            download.Title, download.RetryCount, download.ErrorMessage ?? "Unknown error");

        // Add to blocklist to prevent re-grabbing the same release
        // For torrents: use TorrentInfoHash
        // For Usenet: use Title + Indexer combination
        BlocklistItem? existingBlock = null;

        if (!string.IsNullOrEmpty(download.TorrentInfoHash))
        {
            existingBlock = await db.Blocklist
                .FirstOrDefaultAsync(b => b.TorrentInfoHash == download.TorrentInfoHash);
        }
        else if (!string.IsNullOrEmpty(download.Title))
        {
            // For Usenet, match by title and indexer
            existingBlock = await db.Blocklist
                .FirstOrDefaultAsync(b => b.Title == download.Title &&
                                         b.Indexer == (download.Indexer ?? "Unknown") &&
                                         b.Protocol == "Usenet");
        }

        if (existingBlock == null)
        {
            var blocklistItem = new BlocklistItem
            {
                EventId = download.EventId,
                Title = download.Title,
                TorrentInfoHash = download.TorrentInfoHash, // null for Usenet
                Indexer = download.Indexer ?? "Unknown",
                Protocol = download.Protocol ?? (string.IsNullOrEmpty(download.TorrentInfoHash) ? "Usenet" : "Torrent"),
                Reason = BlocklistReason.FailedDownload,
                Message = download.ErrorMessage ?? "Download failed",
                BlockedAt = DateTime.UtcNow
            };

            db.Blocklist.Add(blocklistItem);
            _logger.LogInformation("[Download Processing] Added to blocklist: {Title} ({Protocol})",
                download.Title, blocklistItem.Protocol);
        }

        // Remove from download client if configured in the client's settings
        if (download.DownloadClient?.RemoveFailedDownloads == true)
        {
            try
            {
                await downloadClientService.RemoveDownloadAsync(
                    download.DownloadClient,
                    download.DownloadId,
                    deleteFiles: true); // Clean up failed download files

                _logger.LogDebug("[Download Processing] Removed failed download from client: {Title}", download.Title);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[Download Processing] Failed to remove failed download from client: {Title}", download.Title);
            }
        }

        // Retry if enabled and under retry limit (respects interactive vs automatic search setting)
        var shouldRedownload = download.IsManualSearch ? redownloadFailedFromInteractive : redownloadFailed;
        if (shouldRedownload && download.RetryCount < 3)
        {
            _logger.LogInformation("[Download Processing] Will retry download on next search cycle: {Title}", download.Title);
            // The automatic search service will pick this up
            download.Status = DownloadStatus.Failed; // Keep as failed but allow retry
        }
        else if (download.RetryCount >= 3)
        {
            _logger.LogWarning("[Download Processing] Max retries reached for: {Title}", download.Title);
            download.ErrorMessage = $"Max retries (3) reached. {download.ErrorMessage}";
        }

        await db.SaveChangesAsync();
    }
}

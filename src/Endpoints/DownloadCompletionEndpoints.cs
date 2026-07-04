using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.EntityFrameworkCore;
using Sportarr.Api.Data;
using Sportarr.Api.Models;
using Sportarr.Api.Services;
using System.Text.Json;

namespace Sportarr.Api.Endpoints;

public static class DownloadCompletionEndpoints
{
    public static IEndpointRouteBuilder MapDownloadCompletionEndpoints(this IEndpointRouteBuilder app)
    {
        // POST /api/v3/download/completed
        // Called by qBittorrent's "Run external program on torrent completion" hook and
        // SABnzbd's post-processing script the instant a download finishes, instead of
        // waiting for EnhancedDownloadMonitorService's next 30s poll tick.
        //
        // Why this matters: near-simultaneous releases for the same event (e.g. two R4E
        // items a few seconds apart) are each evaluated against "is this better than what
        // I currently hold" using a fresh DB read at grab-decision time — but that decision
        // only reflects an earlier accepted grab once ITS download has finished and been
        // imported. Waiting up to 30s for the poller to even notice a completion widens
        // that window; this endpoint collapses it to whatever the download client's own
        // completion hook latency is (typically sub-second).
        //
        // Body: {"downloadId": "<torrent info hash or SAB nzo_id>"}
        // Always returns 200 - a miss (unknown/not-yet-tracked id) is not an error, the
        // 30s poller and external-download detector remain the backstop.
        app.MapPost("/api/v3/download/completed", async (
            HttpContext context,
            SportarrDbContext db,
            DownloadClientService downloadClientService,
            FileImportService fileImportService,
            DownloadProcessingService downloadProcessingService,
            ConfigService configService,
            ILogger<Program> logger) =>
        {
            using var reader = new StreamReader(context.Request.Body);
            var body = await reader.ReadToEndAsync();

            string? downloadId = null;
            try
            {
                using var doc = JsonDocument.Parse(body);
                if (doc.RootElement.TryGetProperty("downloadId", out var idProp))
                    downloadId = idProp.GetString();
            }
            catch (JsonException ex)
            {
                logger.LogWarning(ex, "[Download Completion Webhook] Failed to parse request body: {Body}", body);
                return Results.BadRequest(new { error = "Invalid JSON body - expected {\"downloadId\": \"<hash or nzo id>\"}" });
            }

            if (string.IsNullOrWhiteSpace(downloadId))
            {
                return Results.BadRequest(new { error = "downloadId is required" });
            }

            logger.LogInformation("[Download Completion Webhook] Received completion signal for downloadId: {DownloadId}", downloadId);

            // Case-insensitive match - qBittorrent/SABnzbd can return the hash/nzo_id in a
            // different case between the initial add response and later completion calls
            // (same reasoning as the external-download dedup in EnhancedDownloadMonitorService).
            var download = await db.DownloadQueue
                .Include(d => d.DownloadClient)
                .Include(d => d.Event)
                .FirstOrDefaultAsync(d => d.DownloadId != null && d.DownloadId.ToLower() == downloadId.ToLower());

            if (download == null)
            {
                logger.LogDebug("[Download Completion Webhook] No matching DownloadQueue row for downloadId {DownloadId} - ignoring (30s poller will catch it)", downloadId);
                return Results.Ok(new { matched = false });
            }

            if (download.Status == DownloadStatus.Imported)
            {
                return Results.Ok(new { matched = true, alreadyImported = true, title = download.Title });
            }

            var config = await configService.GetConfigAsync();

            try
            {
                // CancellationToken.None: this import must not be tied to the webhook
                // HTTP request's lifetime - a download client that fires-and-forgets its
                // completion hook (doesn't wait for our response) must not abort a
                // still-running import.
                await downloadProcessingService.ProcessDownloadAsync(
                    download,
                    downloadClientService,
                    fileImportService,
                    db,
                    config.EnableCompletedDownloadHandling,
                    config.RedownloadFailedDownloads,
                    config.RedownloadFailedFromInteractiveSearch,
                    CancellationToken.None);

                await db.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[Download Completion Webhook] Error processing immediate completion for {Title}", download.Title);
                // Don't fail the webhook call - the 30s poller retries this download
                // on its next pass regardless of what happened here.
            }

            return Results.Ok(new { matched = true, title = download.Title, status = download.Status.ToString() });
        });

        return app;
    }
}

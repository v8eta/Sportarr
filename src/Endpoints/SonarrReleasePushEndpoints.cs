using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Sportarr.Api.Data;
using Sportarr.Api.Models;
using Sportarr.Api.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace Sportarr.Api.Endpoints;

/// <summary>
/// Sonarr-compatible "Push Release" endpoint (POST /api/v3/release/push).
///
/// Lets autobrr — or anything that speaks Sonarr's release-push API — hand
/// Sportarr an exact release the instant it's announced on IRC, instead of
/// waiting for the next scheduled RSS sync (and instead of racing the indexer's
/// torznab feed). Sportarr matches the pushed release to a monitored event,
/// runs the SAME decision engine RSS sync uses (quality/custom-format
/// evaluation, release-profile Ignored/Required/Preferred, dedup against grab
/// history + active queue, and a no-downgrade gate), then grabs it through the
/// normal download-client → queue → history pipeline so it imports, seeds, and
/// upgrades like every other grab.
///
/// Response mirrors Sonarr: 200 with a JSON ARRAY of release-decision objects
/// (Sonarr's /api/v3/release/push returns List&lt;ReleaseResource&gt;). autobrr's
/// native Sonarr client unmarshals the body into []ReleasePushResponse, so the
/// response MUST be an array — returning a bare object trips autobrr's
/// "cannot unmarshal object into Go value of type []sonarr.ReleasePushResponse".
/// </summary>
public static class SonarrReleasePushEndpoints
{
    // Sonarr returns an ARRAY of decisions; wrap our single decision in one.
    private static IResult PushArray(bool approved, IEnumerable<string>? rejections,
        int? eventId = null, string? downloadId = null) =>
        Results.Ok(new[]
        {
            new
            {
                approved,
                rejected = !approved,
                tempRejected = false,
                rejections = (rejections ?? Array.Empty<string>()).ToArray(),
                eventId,
                downloadId
            }
        });

    // Malformed input → 400, but still array-shaped so any parser stays happy.
    private static IResult PushBadRequest(string message) =>
        Results.BadRequest(new[]
        {
            new
            {
                approved = false,
                rejected = true,
                tempRejected = false,
                rejections = new[] { message },
                eventId = (int?)null,
                downloadId = (string?)null
            }
        });

    public static IEndpointRouteBuilder MapSonarrReleasePushEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/v3/release/push", async (
            HttpContext context,
            SportarrDbContext db,
            ReleaseMatchingService matchingService,
            ReleaseEvaluator releaseEvaluator,
            ReleaseProfileService releaseProfileService,
            DownloadClientService downloadClientService,
            ILogger<Program> logger) =>
        {
            using var bodyReader = new StreamReader(context.Request.Body);
            var json = await bodyReader.ReadToEndAsync();
            logger.LogInformation("[RELEASE PUSH] POST /api/v3/release/push - {Json}", json);

            JsonElement root;
            try
            {
                using var doc = JsonDocument.Parse(json);
                root = doc.RootElement.Clone();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[RELEASE PUSH] Invalid JSON body");
                return PushBadRequest("Invalid JSON body");
            }

            // Sonarr/autobrr field names vary in casing; accept the common spellings.
            string? Get(params string[] names)
            {
                foreach (var n in names)
                {
                    if (root.TryGetProperty(n, out var el) && el.ValueKind != JsonValueKind.Null)
                        return el.ValueKind == JsonValueKind.String ? el.GetString() : el.ToString();
                }
                return null;
            }

            var title = Get("title", "Title");
            var downloadUrl = Get("downloadUrl", "DownloadUrl", "magnetUrl", "MagnetUrl", "link");
            if (string.IsNullOrWhiteSpace(title) || string.IsNullOrWhiteSpace(downloadUrl))
                return PushBadRequest("title and downloadUrl are required");

            // Sonarr push sends protocol = "torrent"/"usenet"; normalize to Sportarr's "Torrent"/"Usenet".
            var protoRaw = (Get("protocol", "Protocol", "downloadProtocol", "DownloadProtocol") ?? "torrent").ToLowerInvariant();
            var protocol = protoRaw.Contains("usenet") ? "Usenet" : "Torrent";

            long.TryParse(Get("size", "Size"), out var size);
            var publishDate = DateTime.UtcNow;
            var pd = Get("publishDate", "PublishDate");
            if (!string.IsNullOrEmpty(pd))
                DateTime.TryParse(pd, null, System.Globalization.DateTimeStyles.AdjustToUniversal, out publishDate);

            var release = new ReleaseSearchResult
            {
                Title = title!,
                Guid = Get("guid", "Guid") ?? downloadUrl!,
                DownloadUrl = downloadUrl!,
                InfoUrl = Get("infoUrl", "InfoUrl"),
                Indexer = Get("indexer", "Indexer") ?? "autobrr (push)",
                TorrentInfoHash = Get("infoHash", "InfoHash", "torrentInfoHash"),
                Protocol = protocol,
                Size = size,
                PublishDate = publishDate,
            };

            // Prowlarr/base64 links sometimes carry trailing newlines (matches /api/release/grab).
            if (release.DownloadUrl.Contains('\n') || release.DownloadUrl.Contains('\r'))
                release.DownloadUrl = release.DownloadUrl.Replace("\n", "").Replace("\r", "").Trim();

            // --- 1. Match the release to a monitored, aired event (same matcher RSS sync uses) ---
            var nowUtc = DateTime.UtcNow;
            var monitoredEvents = await db.Events
                .Include(e => e.League).ThenInclude(l => l!.RootFolder)
                .Include(e => e.HomeTeam).Include(e => e.AwayTeam)
                .Where(e => e.Monitored && e.League != null && e.EventDate <= nowUtc
                    && e.Status != "Postponed" && e.Status != "postponed"
                    && e.Status != "Cancelled" && e.Status != "cancelled"
                    && e.Status != "Canceled" && e.Status != "canceled")
                .ToListAsync();

            var preParsed = matchingService.ParseRelease(release.Title);
            Event? matched = null;
            var bestConfidence = int.MinValue;
            foreach (var evt in monitoredEvents)
            {
                var mr = matchingService.ValidateRelease(release, evt, null, false, preParsed);
                if (!mr.IsMatch || mr.IsHardRejection)
                    continue;
                if (mr.Confidence > bestConfidence)
                {
                    bestConfidence = mr.Confidence;
                    matched = evt;
                }
            }

            if (matched == null)
            {
                logger.LogInformation("[RELEASE PUSH] No monitored event matched '{Title}' — rejecting", title);
                return PushArray(false, new[] { "No matching monitored event" });
            }
            logger.LogInformation("[RELEASE PUSH] '{Title}' matched event {EventId} '{Event}' (confidence {Confidence})",
                title, matched.Id, matched.Title, bestConfidence);

            // --- 2. Evaluate quality + custom formats against the event's quality profile ---
            var qualityProfile = matched.QualityProfileId.HasValue
                ? await db.QualityProfiles.FirstOrDefaultAsync(p => p.Id == matched.QualityProfileId.Value)
                : await db.QualityProfiles.OrderBy(q => q.Id).FirstOrDefaultAsync();
            if (qualityProfile != null)
            {
                var customFormats = await db.CustomFormats.ToListAsync();
                var evaluation = releaseEvaluator.EvaluateRelease(
                    release, qualityProfile, customFormats,
                    requestedPart: null, sport: matched.Sport, enableMultiPartEpisodes: false);
                release.Quality = evaluation.Quality;
                release.QualityScore = evaluation.QualityScore;
                release.CustomFormatScore = evaluation.CustomFormatScore;
                release.Score = evaluation.TotalScore;
                release.Approved = evaluation.Approved && !evaluation.Rejections.Any();
                release.Rejections = evaluation.Rejections;
                if (release.Rejections.Any())
                {
                    logger.LogInformation("[RELEASE PUSH] Rejected by quality evaluation: {Reasons}", string.Join(", ", release.Rejections));
                    return PushArray(false, release.Rejections);
                }
            }

            // --- 3. Release-profile filter (Ignored / Required keywords, Preferred score) ---
            var profiles = await releaseProfileService.LoadReleaseProfilesAsync();
            if (profiles.Any())
            {
                var pe = releaseProfileService.EvaluateRelease(release, profiles, matched.League?.Tags);
                if (pe.IsRejected)
                {
                    logger.LogInformation("[RELEASE PUSH] Rejected by release profile: {Reasons}", string.Join(", ", pe.Rejections));
                    return PushArray(false, pe.Rejections);
                }
                release.CustomFormatScore += pe.PreferredScore;
                release.Score += pe.PreferredScore;
            }

            // --- 4. Dedup: already grabbed (history) or already in an active queue item ---
            var hash = release.TorrentInfoHash;
            var already = false;
            if (!string.IsNullOrEmpty(hash))
            {
                already = await db.GrabHistory.AnyAsync(g => g.EventId == matched.Id && g.TorrentInfoHash == hash && !g.Superseded)
                       || await db.DownloadQueue.AnyAsync(d => d.EventId == matched.Id && d.TorrentInfoHash == hash
                              && (d.Status == DownloadStatus.Queued || d.Status == DownloadStatus.Downloading));
            }
            if (!already && !string.IsNullOrEmpty(release.Guid))
                already = await db.GrabHistory.AnyAsync(g => g.EventId == matched.Id && g.Guid == release.Guid && !g.Superseded);
            if (already)
            {
                logger.LogInformation("[RELEASE PUSH] '{Title}' already grabbed/queued for event {EventId}", title, matched.Id);
                return PushArray(false, new[] { "Already grabbed or queued" });
            }

            // --- 5. No-downgrade gate: if the event already has a recognised file, require a higher score ---
            await db.Entry(matched).Collection(e => e.Files).LoadAsync();
            var existingFile = matched.Files.FirstOrDefault(f => f.PartName == null && f.Exists);
            if (existingFile != null)
            {
                var existingScore = ReleaseEvaluator.CalculateQualityScoreFromName(existingFile.Quality) + existingFile.CustomFormatScore;
                var newScore = ReleaseEvaluator.CalculateQualityScoreFromName(release.Quality) + release.CustomFormatScore;
                if (existingScore > 0 && newScore <= existingScore)
                {
                    logger.LogInformation("[RELEASE PUSH] '{Title}' not an upgrade for event {EventId} ({New} <= {Existing})",
                        title, matched.Id, newScore, existingScore);
                    return PushArray(false, new[] { $"Existing file scores higher or equal ({existingScore})" });
                }
            }

            // --- 6. Grab via the normal download-client + queue + history pipeline ---
            var supportedTypes = DownloadClientService.GetClientTypesForProtocol(release.Protocol);
            var downloadClient = await db.DownloadClients
                .Where(dc => dc.Enabled && supportedTypes.Contains(dc.Type))
                .OrderBy(dc => dc.Priority)
                .FirstOrDefaultAsync();
            if (downloadClient == null)
                return PushArray(false, new[] { $"No enabled {release.Protocol} download client configured" });

            var indexerRecord = !string.IsNullOrEmpty(release.Indexer)
                ? await db.Indexers.FirstOrDefaultAsync(i => i.Name == release.Indexer)
                : null;
            var grabCategory = !string.IsNullOrWhiteSpace(matched.League?.RootFolder?.DefaultDownloadClientCategory)
                ? matched.League!.RootFolder!.DefaultDownloadClientCategory!
                : downloadClient.Category;

            AddDownloadResult downloadResult;
            try
            {
                downloadResult = await downloadClientService.AddDownloadWithResultAsync(
                    downloadClient, release.DownloadUrl, grabCategory, release.Title,
                    indexerRecord?.SeedRatio, indexerRecord?.SeedTime);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[RELEASE PUSH] Exception adding download");
                return PushArray(false, new[] { $"Download client error: {ex.Message}" });
            }

            if (!downloadResult.Success || downloadResult.DownloadId == null)
            {
                logger.LogWarning("[RELEASE PUSH] Download client rejected '{Title}': {Error}", title, downloadResult.ErrorMessage);
                return PushArray(false, new[] { downloadResult.ErrorMessage ?? "Download client rejected the release" });
            }

            // Supersede previous grabs for this event (full-event part) so the old file isn't re-grabbed.
            foreach (var old in await db.GrabHistory
                .Where(g => g.EventId == matched.Id && g.PartName == null && !g.Superseded).ToListAsync())
            {
                old.Superseded = true;
            }

            var pushedQueueItem = new DownloadQueueItem
            {
                EventId = matched.Id,
                Title = release.Title,
                DownloadId = downloadResult.DownloadId,
                DownloadClientId = downloadClient.Id,
                Status = DownloadStatus.Queued,
                Quality = release.Quality,
                Codec = release.Codec,
                Source = release.Source,
                Size = release.Size,
                Downloaded = 0,
                Progress = 0,
                Indexer = release.Indexer,
                IndexerId = indexerRecord?.Id,
                Protocol = release.Protocol,
                TorrentInfoHash = release.TorrentInfoHash,
                RetryCount = 0,
                LastUpdate = DateTime.UtcNow,
                QualityScore = release.QualityScore,
                CustomFormatScore = release.CustomFormatScore,
                IsManualSearch = false
            };
            db.DownloadQueue.Add(pushedQueueItem);

            db.GrabHistory.Add(new GrabHistory
            {
                EventId = matched.Id,
                Title = release.Title,
                Indexer = release.Indexer ?? "",
                IndexerId = indexerRecord?.Id,
                DownloadUrl = release.DownloadUrl,
                Guid = release.Guid,
                Protocol = release.Protocol,
                TorrentInfoHash = release.TorrentInfoHash,
                Size = release.Size,
                Quality = release.Quality,
                Codec = release.Codec,
                Source = release.Source,
                QualityScore = release.QualityScore,
                CustomFormatScore = release.CustomFormatScore,
                GrabbedAt = DateTime.UtcNow,
                DownloadClientId = downloadClient.Id,
                DownloadId = downloadResult.DownloadId
            });

            await db.SaveChangesAsync();

            // Immediate status check so the grab shows on the Activity page right away
            // (parity with /api/release/grab) instead of waiting for the next monitor poll.
            try
            {
                await Task.Delay(2000);
                var status = await downloadClientService.GetDownloadStatusAsync(downloadClient, downloadResult.DownloadId);
                if (status != null)
                {
                    pushedQueueItem.Status = status.Status switch
                    {
                        "downloading" => DownloadStatus.Downloading,
                        "paused" => DownloadStatus.Paused,
                        "completed" => DownloadStatus.Completed,
                        "queued" or "waiting" => DownloadStatus.Queued,
                        _ => DownloadStatus.Queued
                    };
                    pushedQueueItem.Progress = status.Progress;
                    pushedQueueItem.Downloaded = status.Downloaded;
                    pushedQueueItem.Size = status.Size > 0 ? status.Size : release.Size;
                    pushedQueueItem.LastUpdate = DateTime.UtcNow;
                    await db.SaveChangesAsync();
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[RELEASE PUSH] Initial status check failed (download will be tracked by the monitor)");
            }

            logger.LogInformation("[RELEASE PUSH] ✓ Grabbed '{Title}' for event {EventId} '{Event}' via {Client}",
                title, matched.Id, matched.Title, downloadClient.Name);

            return PushArray(true, null, matched.Id, downloadResult.DownloadId);
        });

        return app;
    }
}

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;

namespace Sportarr.Api.Endpoints;

public static class SonarrSystemEndpoints
{
    public static IEndpointRouteBuilder MapSonarrSystemEndpoints(this IEndpointRouteBuilder app, string dataPath)
    {
        // GET /api/v3/system/status - System status (Sonarr v3 API for Prowlarr)
        app.MapGet("/api/v3/system/status", (HttpContext context, ILogger<Program> logger) =>
        {
            // Prowlarr polls this endpoint on a cadence for connectivity checks.
            // Request-logging middleware already records every inbound HTTP call at
            // Info, so a second per-call announcement here is pure duplication.
            logger.LogDebug("[PROWLARR] GET /api/v3/system/status - Prowlarr requesting system status (v3 API)");

            return Results.Ok(new
            {
                // Report as "Sonarr" so Sonarr-API clients (autobrr's arr push/test,
                // etc.) accept this v3 endpoint as a real Sonarr. instanceName keeps
                // the Sportarr identity for humans. Prowlarr already syncs to it as a
                // Sonarr app, so this is consistent.
                appName = "Sonarr",
                instanceName = "Sportarr",
                version = Sportarr.Api.Version.AppVersion,
                buildTime = DateTime.UtcNow,
                isDebug = false,
                isProduction = true,
                isAdmin = false,
                isUserInteractive = false,
                startupPath = Directory.GetCurrentDirectory(),
                appData = dataPath,
                osName = RuntimeInformation.OSDescription,
                osVersion = Environment.OSVersion.VersionString,
                isNetCore = true,
                isLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux),
                isOsx = RuntimeInformation.IsOSPlatform(OSPlatform.OSX),
                isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows),
                isDocker = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true",
                mode = "console",
                branch = "main",
                authentication = "forms",
                sqliteVersion = "3.0",
                urlBase = "",
                runtimeVersion = Environment.Version.ToString(),
                runtimeName = ".NET",
                startTime = DateTime.UtcNow
            });
        });

        // GET /api/v3/health - Health check endpoint for Decypharr validation
        app.MapGet("/api/v3/health", (HttpContext context, ILogger<Program> logger) =>
        {
            logger.LogDebug("[DECYPHARR] GET /api/v3/health - Health check requested");
            return Results.Ok(Array.Empty<object>());
        });

        return app;
    }
}

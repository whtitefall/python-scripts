param(
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$galleryDl = Join-Path $projectRoot "backend\.venv\Scripts\gallery-dl.exe"
$archiveFile = Join-Path $projectRoot "backend\scripts\.x_gallery_archive.txt"
$destDir = "C:\Users\whtit\OneDrive\Desktop\tvideo"
$ffmpegBin = "C:\Users\whtit\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin"

if (-not (Test-Path $galleryDl)) {
    throw "gallery-dl not found: $galleryDl"
}

if (-not (Test-Path $ffmpegBin)) {
    throw "ffmpeg bin not found: $ffmpegBin"
}

New-Item -ItemType Directory -Force -Path $destDir | Out-Null
if (-not (Test-Path $archiveFile)) {
    New-Item -ItemType File -Path $archiveFile | Out-Null
}

# Make ffmpeg/ffprobe visible to yt-dlp in this process.
$env:PATH = "$ffmpegBin;$env:PATH"

$args = @(
    "--cookies-from-browser", "firefox",
    "--download-archive", $archiveFile,
    "-D", $destDir,
    "--filter", "type == 'video'",
    "--post-range", "1-200",
    "-A", "80",
    "--sleep-extractor", "8-15",
    "--sleep-request", "1.2-2.5",
    "--sleep", "1.5-3.5",
    "--sleep-429", "60-180",
    "-r", "1M",
    "-o", "extractor.twitter.videos=ytdl",
    "-o", "extractor.ytdl.format=bestvideo*+bestaudio/best",
    "-o", "extractor.ytdl.cmdline-args=--sleep-interval 2 --max-sleep-interval 5 --sleep-requests 1 --concurrent-fragments 1 --limit-rate 1M",
    "https://x.com/i/bookmarks"
)

if ($DryRun) {
    & $galleryDl "--simulate" @args
}
else {
    & $galleryDl @args
}

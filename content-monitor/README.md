# Content Updates Monitor

独立于职位监控脚本的内容更新监控器，当前支持：
- ByteByteGo 博客 RSS
- YouTube 频道更新（通过频道页自动解析 feed）

## 1) 安装

```powershell
cd "C:\Users\whtit\OneDrive\Documents\New project\python-scripts-repo"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r content-monitor/requirements.txt
```

## 2) 配置邮箱

```powershell
$env:SMTP_USER="你的Gmail地址"
$env:SMTP_PASSWORD="你的16位AppPassword"
```

## 3) 配置监控源

编辑 `content-monitor/sources.json`：
- `poll_interval_minutes`：轮询间隔（分钟）
- `max_post_age_days_for_email`：仅发送最近 N 天发布内容
- `sources.rss`：RSS/Atom 源
- `sources.youtube`：YouTube 源（支持 `feed_url`、`channel_id` 或 `channel_url`）

## 4) 运行

首次只建立基线（默认不发送历史内容）：

```powershell
python content-monitor/content_monitor.py --once
```

首次希望发送当前可见内容：

```powershell
python content-monitor/content_monitor.py --once --send-initial-snapshot
```

本地调试（不发邮件）：

```powershell
python content-monitor/content_monitor.py --once --dry-run
```

## 5) 状态文件

默认状态文件：`content-monitor/.content_monitor_state.json`  
用于去重与失败重试（邮件发送失败会保留待发送队列）。


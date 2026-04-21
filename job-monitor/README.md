# Canada Software Developer Job Monitor

一个可长期运行的 Python 脚本，用于监控加拿大 `software developer` 相关职位，并发送邮件通知。

当前已接入公司源：
- Google（Google Careers 页面解析）
- Microsoft（`apply.careers.microsoft.com/api/pcsx/search`）
- Workday（官方 Workday CXS）
- Qualcomm（Workday CXS，当前可能返回 0 职位）
- Instacart（Greenhouse）
- Robinhood（Greenhouse）
- DoorDash（Greenhouse）
- Pinterest（Greenhouse）
- Spotify（Lever）

当前暂不可稳定接入：
- Uber（`jobs.uber.com` 在无浏览器挑战场景下持续 403）

## 1) 安装

```powershell
cd "C:\Users\whtit\OneDrive\Documents\New project"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2) 配置邮箱发送

脚本使用 Gmail SMTP。请先在 Google 账号里开启 2FA，并创建 **App Password**。

在当前 PowerShell 会话设置：

```powershell
$env:SMTP_USER="你的Gmail地址"
$env:SMTP_PASSWORD="你的16位AppPassword"
```

说明：
- 收件人已默认设置为 `whtitefall@gmail.com`，在 `companies.json` 的 `recipient_email` 字段。
- 发件人来自 `SMTP_USER`。

## 3) 配置监控源

编辑 `companies.json`：
- `poll_interval_minutes`：轮询间隔（分钟）
- `request_delay_seconds` + `request_jitter_seconds`：请求节流，降低反爬风险
- `sources.google_careers`：Google Careers
- `sources.microsoft_careers`：Microsoft Eightfold API
- `sources.workday_cxs`：Workday CXS API
- `sources.greenhouse`：Greenhouse API
- `sources.lever`：Lever API
- 每个源都支持 `title_keywords` 进行标题二次过滤

脚本会做两层过滤：
- 地点必须匹配加拿大
- 职位标题必须命中 `title_keywords`
- 职位标题命中 `exclude_title_keywords` 时会被排除（例如 `principal/staff/tester`）
- 当职位描述/资格里出现 `5 years`、`5+ years` 及以上经验要求时，会按 `exclude_required_experience_years_at_or_above` 自动排除
- 邮件中的发布时间会显示为“距今分钟数 + 具体 UTC 时间”

## 4) 运行

单次检查（测试用）：

```powershell
python .\job_monitor.py --once
```

首次运行如果希望把“当前已有岗位”也发邮件：

```powershell
python .\job_monitor.py --once --send-initial-snapshot
```

全天持续监控（循环运行）：

```powershell
python .\job_monitor.py
```

## 5) 状态文件

脚本会生成 `.job_monitor_state.json`，用于记住已通知过的岗位，避免重复发信。

## 6) 常见问题

- 如果看到 `SMTP_USER / SMTP_PASSWORD 未配置`，说明环境变量还没设置。
- 如果某个源返回 403/404，脚本会记录 warning 并继续跑其他源。
- 首次运行默认只建立基线，不会发历史全量职位；使用 `--send-initial-snapshot` 可以首次全量推送。

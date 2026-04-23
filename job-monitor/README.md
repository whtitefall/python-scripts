# Canada Software Developer Job Monitor

一个可长期运行的 Python 脚本，用于监控加拿大 `software developer` 相关职位，并发送邮件通知。

当前已接入公司源：
- Google（Google Careers 页面解析）
- Microsoft（`apply.careers.microsoft.com/api/pcsx/search`）
- Uber（`uber.com` 官方 Careers API：`loadFilterOptions` + `loadSearchJobsResults`）
- Qualcomm Careers（`careers.qualcomm.com/api/pcsx/search`）
- Workday（官方 Workday CXS）
- Qualcomm（Workday CXS，当前可能返回 0 职位；作为冗余源保留）
- AMD（Jibe / iCIMS API）
- Yelp（`yelp.careers` 搜索页内嵌职位数据 + iCIMS apply URL）
- Amazon（`amazon.jobs/en/search.json`）
- Confluent（Ashby，`jobs.ashbyhq.com/confluent`）
- IBM（`www-api.ibm.com` careers 搜索 API）
- Instacart（Greenhouse）
- Robinhood（Greenhouse）
- DoorDash（Greenhouse）
- Pinterest（Greenhouse）
- Spotify（Lever）

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
- `max_post_age_days_for_email`：仅推送最近 N 天发布的岗位（默认 2）
- `request_delay_seconds` + `request_jitter_seconds`：请求节流，降低反爬风险
- `sources.google_careers`：Google Careers
- `sources.microsoft_careers`：Microsoft Eightfold API
- `sources.uber_careers`：Uber Careers API
- `sources.workday_cxs`：Workday CXS API
- `sources.jibe`：Jibe / iCIMS API（如 AMD）
- `sources.yelp_careers`：Yelp Careers（Phenom + iCIMS）
- `sources.amazon_jobs`：Amazon Jobs JSON API
- `sources.ashby`：Ashby Jobs（如 Confluent）
- `sources.ibm_careers_api`：IBM Careers Search API
- `sources.greenhouse`：Greenhouse API
- `sources.lever`：Lever API
- 每个源都支持 `title_keywords` 进行标题二次过滤
- `ai_filter`：GitHub Models 二次智能过滤（可选）

脚本会做两层过滤：
- 地点必须匹配加拿大
- 职位标题必须命中 `title_keywords`
- 职位标题命中 `exclude_title_keywords` 时会被排除（例如 `principal/staff/tester`）
- 若设置 `exclude_required_experience_years_at_or_above`，会按该阈值做硬过滤；当前配置已关闭该硬过滤，交由 AI 二次判断
- 邮件中的发布时间会显示为“X天Y小时Z分前 + 具体 UTC 时间”

### AI 智能过滤（GitHub Models）

脚本支持“优先 AI 判定”：
- 先做基础规则过滤（地点/标题关键词/排除关键词）
- AI 再做主判定（支持领域偏好与硬件岗位忽略）

`companies.json` 中的 `ai_filter` 字段可控制：
- `enabled`：是否启用
- `priority_mode`：为 `true` 时优先走 AI（会评估本轮全部职位）
- `model`：模型 ID（默认 `openai/gpt-4o`）
- `max_jobs_per_cycle`：每轮最多给 AI 判定多少个职位
- `max_detail_chars`：每个职位传给 AI 的详情文本长度上限
- `fallback_allow_on_error`：AI 调用失败时是否回退为“放行”
- `preferred_min_experience_years`：AI 偏好/接受的经验下限（例如 3 年）
- `preferred_role_domains`：AI 优先领域（如 web/AI/LLM/前后端）
- `ignore_role_domains`：AI 忽略方向（如硬件相关）
- `custom_instruction`：给 AI 的额外自然语言偏好

在 GitHub Actions 中已自动配置 `models: read` 权限，并通过 `GITHUB_TOKEN` 调用 GitHub Models 推理 API。

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
在 GitHub Actions 中，该状态文件会自动通过缓存恢复和保存，实现跨运行持久化增量监控。

## 6) 常见问题

- 如果看到 `SMTP_USER / SMTP_PASSWORD 未配置`，说明环境变量还没设置。
- 如果某个源返回 403/404，脚本会记录 warning 并继续跑其他源。
- 首次运行默认只建立基线，不会发历史全量职位；使用 `--send-initial-snapshot` 可以首次全量推送。

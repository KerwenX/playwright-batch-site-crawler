# Playwright Batch Site Crawler

基于本地 `Python 3.14` 和 Playwright 的站点级分层爬虫。

程序会：

- 从 `config.json` 读取配置
- 从 `input_urls.txt` 读取种子 URL
- 按站点去重后批量执行
- 每个站点输出到独立目录
- 支持断点续传
- 跳过已完成站点
- 额外输出全局汇总和 CSV 文件

## 已支持的站点形态

- 通用传统站点：服务端渲染、普通 `<a>` 链接
- `*.ajcass.com` 家族：Hash 路由、SPA、接口驱动列表、动态点击展开
- `*.cbpt.cnki.net` 家族：CNKI/CBPT 传统期刊站

`*.cbpt.cnki.net` 当前会额外处理这些场景：

- `wkTextContent.aspx` 的目录页、年卷页、期次页、栏目索引页
- `wkList.aspx` 的列表页和导航页
- 编辑后台、验证码页、下载接口会记录，但默认不继续访问
- `paperDigest.aspx`、`wkTextContent.aspx?contentID=...` 这类正文叶子页默认只记录 URL，不逐个打开

已验证示例：

- `https://zgncjj.ajcass.com/#/`
- `https://erj.ajcass.com/#/index`
- `https://jjgl.ajcass.com/`
- `https://zgfx.cbpt.cnki.net/`
- `https://ddjy.cbpt.cnki.net/`

## 环境要求

- Windows PowerShell
- `C:\Python314\python.exe`
- 已安装 `playwright`
- 已安装 Chromium 内核

安装浏览器内核：

```powershell
& 'C:\Python314\python.exe' -m playwright install chromium
```

## 配置

编辑 [config.json](/D:/Desktop/qoder%20work/config.json)：

```json
{
  "input_urls_file": "input_urls.txt",
  "output_root": "crawl_output_batch",
  "chromium_executable_path": "",
  "log_level": "INFO",
  "log_to_file": true,
  "headless": true,
  "max_concurrency": 8,
  "page_timeout_ms": 20000,
  "settle_ms": 900,
  "max_pages_per_site": 0,
  "checkpoint_every_pages": 10,
  "checkpoint_every_seconds": 30,
  "skip_completed_sites": true,
  "visit_leaf_pages": false,
  "include_site_homepage_seed": true,
  "enable_generic_interactions": true,
  "max_interaction_clicks_per_page": 18,
  "max_api_pages_per_series": 0
}
```

关键配置说明：

- `chromium_executable_path = ""`：留空时使用 Playwright 默认 Chromium；填写后使用你指定的 `chrome.exe` / `chromium.exe`
- `log_level = "INFO"`：默认打印关键运行日志；改成 `"DEBUG"` 可看到更细的 URL 入队和发现细节
- `log_to_file = true`：除了控制台，还会把日志写入文件
- `max_pages_per_site = 0`：不限制页面访问数
- `visit_leaf_pages = false`：详情页 URL 记录下来，但默认不逐个打开
- `enable_generic_interactions = true`：开启通用交互探测
- `max_interaction_clicks_per_page`：每页最多做多少次交互点击
- `max_api_pages_per_series = 0`：单个分页接口不设上限；如果只想快测，可改成 `10`、`50` 之类
- `skip_completed_sites = true`：已完成站点直接跳过

`chromium_executable_path` 使用说明：

- 可以写绝对路径，例如 `C:\\Program Files\\Chromium\\Application\\chrome.exe`
- 可以写相对路径，程序会按 `config.json` 所在目录解析
- 可以写带环境变量的路径，例如 `%LOCALAPPDATA%\\ms-playwright\\chromium-1208\\chrome-win64\\chrome.exe`
- 如果路径不存在，程序会在启动时直接报错，避免你误以为已经使用了指定浏览器

日志说明：

- 根目录会输出 `batch.log`，记录批量任务入口、跳过已完成站点、站点开始/结束、全局汇总写出位置
- 每个站点目录会输出 `crawl.log`，记录检查点恢复、浏览器启动、批次进度、页面访问开始/结束、异常堆栈、检查点保存
- `INFO` 级别适合日常跑批定位问题
- `DEBUG` 级别适合深入排查某个站点为什么没有入队、为什么某些 URL 没继续抓

## 输入

编辑 [input_urls.txt](/D:/Desktop/qoder%20work/input_urls.txt)，每行一个 URL：

```text
https://zgncjj.ajcass.com/#/
https://erj.ajcass.com/#/index
https://jjgl.ajcass.com/
https://zgfx.cbpt.cnki.net/
```

规则：

- 同一站点在输入文件里出现多次，只会生成一个站点任务
- 站点目录名会自动转成下划线格式
- 例如 `www.baidu.com` 会输出到 `www_baidu_com/`

## 运行

直接运行：

```powershell
& 'C:\Python314\python.exe' ajcass_crawler.py
```

或者：

```powershell
& 'C:\Python314\python.exe' site_batch_crawler.py
```

## 断点续传

每个站点目录下都有 `checkpoint.json`。

- 程序中断后再次运行，会从检查点继续
- 已访问 URL 不会重复抓取
- 已完成站点不会重复跑
- 如果升级了站点规则，程序会在恢复检查点时重新评估已发现 URL，并把现在应继续访问但过去被错误跳过的 URL 重新入队
- 如果设置了 `max_pages_per_site`，达到上限时会保持 `completed = false`

## 输出结构

```text
crawl_output_batch/
  batch.log
  all_discovered_urls.txt
  all_discovered_urls.tsv
  all_discovered_urls.csv
  batch_summary.json
  sites_summary.csv
  zgncjj_ajcass_com/
    all_discovered_urls.txt
    checkpoint.json
    crawl.log
    edges.jsonl
    edges.csv
    external_or_non_queueable_urls.txt
    nodes.jsonl
    nodes.csv
    same_site_urls.txt
    seed_urls.txt
    summary.json
    visits.jsonl
    visits.csv
```

说明：

- 根目录 `all_discovered_urls.txt`：所有站点发现到的 URL 总表
- 根目录 `all_discovered_urls.csv`：适合直接用 Excel 打开
- 根目录 `sites_summary.csv`：按站点汇总
- 站点目录 `nodes.csv` / `edges.csv` / `visits.csv`：单站点明细，适合筛选分析

## 当前入口文件

- [ajcass_crawler.py](/D:/Desktop/qoder%20work/ajcass_crawler.py)
- [site_batch_crawler.py](/D:/Desktop/qoder%20work/site_batch_crawler.py)

`ajcass_crawler.py` 只是入口，核心逻辑都在 `site_batch_crawler.py`。

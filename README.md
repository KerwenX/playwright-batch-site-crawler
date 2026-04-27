# Playwright Batch Site Crawler

基于 Playwright 的站点级分层爬虫，支持：

- 本地 Windows + `Python 3.14`
- 服务器 Docker + `Python 3.8`
- 批量站点任务
- 断点续传
- 代理池轮转
- 同站点全量 BFS
- 同站外链只记录不继续挖掘
- 统一输出目录和站点子目录

当前代码同时维护两套入口：

- 本地入口：[ajcass_crawler.py](/D:/Desktop/qoder%20work/ajcass_crawler.py)
- 服务器入口：[server_batch_crawler.py](/D:/Desktop/qoder%20work/server_batch_crawler.py)
- 核心逻辑：[site_batch_crawler.py](/D:/Desktop/qoder%20work/site_batch_crawler.py)

## 项目结构

当前已经将原来的单文件核心逻辑拆分为包内模块，外部启动方式保持不变：

- `ajcass_crawler.py`
  - 本地运行入口。
- `server_batch_crawler.py`
  - Docker / Python 3.8 服务器入口。
- `site_batch_crawler.py`
  - 兼容层，保留旧的导入路径与启动方式，内部转发到 `crawler_core`。
- `crawler_core/constants.py`
  - 常量、默认配置、站点规则常量。
- `crawler_core/models.py`
  - `BatchConfig`、`SiteConfig`、队列项、访问记录等数据模型。
- `crawler_core/utils.py`
  - URL 规范化、文件写入、日志、代理配置、分组等通用工具函数。
- `crawler_core/site.py`
  - 单站点抓取核心逻辑，包含发现、交互、断点续传、输出落盘。
- `crawler_core/batch.py`
  - 批量站点调度与多站并发入口。
- `crawler_core/cli.py`
  - CLI 级 `main/async_main` 封装。

这样拆分后：
- 本地版和服务器版仍然共用同一套核心代码。
- 后续新增站点规则或调度能力时，不需要继续在一个超大文件里改动。
- 旧脚本、旧命令、旧导入路径不需要改。

## 设计目标

默认策略不是“识别正文页后停止”，而是：

- 只要是同站点、可访问、像 HTML 的 URL，就入队并访问一次。
- 外站 URL、下载资源、明显危险动作 URL 只记录，不继续访问。
- 已访问 URL 不会重复入队，避免死循环。
- 页面存在交互、弹窗、AJAX 分页时，会尽量展开后继续发现 URL。
- 发现队列会优先调度更像目录页、正文页、期次页的 URL；明显低价值的统计、下载控制、样式、登录类链接会延后，避免抢占前几页的访问预算。
- 高并发模式下支持多站点并行、站内动态 worker 调度、共享 Playwright 驱动。
- checkpoint 会额外带上“正在跑但尚未落盘”的页面，避免中断时丢失 in-flight URL。

## 已适配的站点类型

### 1. 通用站点

- 普通多页站
- 基于渲染后的 DOM 抽取链接
- 通用按钮、分页、Tab、More 链接点击探测
- 通用 SPA 脚本路由发现
  - 会从同站 JS bundle 中提取 `path:\"/foo\"` 这类路由
  - 对 hash SPA 会自动转成 `#/foo`
  - 可以避免“首页几乎没有 DOM 链接，导致只抓到根页”的问题
- 通用 URL 清洗与队列优先级
  - 会尽量剔除 HTML 标签误判、坏格式相对链接、明显的统计/下载控制接口
  - 即使某些低价值 URL 不继续访问，也仍然会完整记录在发现结果里

#### 1.1 Boyuan / `uniapp.boyuancb.com` 类站点

这类站通常首页 DOM 链接很少，但会通过接口动态拉年份、期次和文章。

当前已支持：

- 从脚本里发现 `#/browse`、`#/guide`、`#/browse_details` 等前端路由
- 从接口自动扩展：
  - `GetJournalGapYear`
  - `GetJournalYear`
  - `GetThatYearIssueList`
  - `GetJournalIssueList`
  - `GetBackIssueBrowsing`
  - `GetJournalArticleList`
- 自动生成：
  - `#/browse?year=YYYY`
  - `#/browse?year=YYYY&issue=N`
  - `#/browse_details?year=YYYY&issue=N&issuecid=ID`

这类站点例子：

- `http://www.chinacirculation.org`

### 2. `*.ajcass.com`

- Hash 路由 SPA
- 英文页、期次页、文章页
- 接口驱动的目录页发现

### 3. `*.cbpt.cnki.net`

#### 传统 `WKH2/WebPublication` 类站点

- `wkTextContent.aspx`
- `wkList.aspx`
- `paperDigest.aspx`
- 年卷期目录、栏目页、正文页

#### 新版 `portal/journal/portal/client/...` 类站点

重点适配了这类站点的“过刊浏览 / 发文论文 / 排行 / 下载中心 / 新闻栏目”链路。

当前额外支持：

- 解析 `onclick` 中的真实跳转 URL
- 自动识别并展开：
  - `goNewList(...)`
  - `goDownloadList(...)`
  - `goLinkpostList(...)`
  - `guokanTurnPageList(...)`
  - `getChineseHtmlUrl(...)`
  - `getSpecialPDFUrl(...)`
  - `gotoCNKINode(...)`
- 对 `paperPage_list`、`listPrePaperOrNextPaper` 这类仅 AJAX 返回 HTML 的分页接口，直接发起页面内请求并继续抽取 URL
- 对 `portal/common/api/*`、`portal/journal/api/*`、根级 `/api/*` 这类接口只记录不访问，避免浪费访问配额

这类站点的典型例子：

- `https://gggl.cbpt.cnki.net/portal`
- `https://zgfx.cbpt.cnki.net/`
- `https://ddjy.cbpt.cnki.net/`

## 本地环境

要求：

- Windows PowerShell
- `C:\Python314\python.exe`
- 已安装 `playwright`
- 已安装 Chromium

安装浏览器内核：

```powershell
& 'C:\Python314\python.exe' -m playwright install chromium
```

## 服务器 Docker 环境

服务器入口默认读取 `config.server.json`，也可以通过环境变量覆盖：

- 环境变量：`CRAWLER_CONFIG_PATH`
- 入口脚本：[server_batch_crawler.py](/D:/Desktop/qoder%20work/server_batch_crawler.py)

推荐运行方式：

```bash
docker run --rm -it \
  --network host \
  -e PYTHONUNBUFFERED=1 \
  -e CRAWLER_CONFIG_PATH=config.server.json \
  --shm-size=1g \
  -v /opt/huawei/data2/w00943222/spider:/opt/huawei/data2/w00943222/spider \
  -w /opt/huawei/data2/w00943222/spider \
  --entrypoint python3 \
  playwright_focal:3.21.2 \
  ./server_batch_crawler.py
```

## 高并发优化

这一版针对“站点很多、需要持续跑批”的场景，核心改动有四个：

- `max_site_concurrency`
  站点级并发，多个站点可以同时跑，不再串行等待。
- 站内动态 worker
  不再按固定 batch 整批等待，而是哪个页面先结束就立刻补下一个，减少慢页拖尾。
- 轻量 checkpoint
  `write_full_outputs_on_checkpoint = false` 时，周期性 checkpoint 只刷新 `summary.json` 和 `checkpoint.json`，大幅降低高并发场景下的磁盘写放大。
- 共享 Playwright
  批任务级只启动一次 Playwright driver，各站点复用，减少频繁启动浏览器驱动的额外开销。

服务器建议优先从下面这组参数开始调：

- `max_site_concurrency = 4`
- `max_concurrency = 12`
- `proxy_session_count = 12`
- `checkpoint_every_pages = 100`
- `checkpoint_every_seconds = 180`
- `write_full_outputs_on_checkpoint = false`

如果目标站更重、代理质量一般，优先降低：

- `max_site_concurrency`
- `max_concurrency`
- `max_interaction_clicks_per_page`

## 配置

本地默认配置文件：[config.json](/D:/Desktop/qoder%20work/config.json)

服务器示例配置文件：[config.server.example.json](/D:/Desktop/qoder%20work/config.server.example.json)

常用字段：

- `input_urls_file`
  批量种子 URL 文件，一行一个 URL。
- 配置文件和输入文件支持 `UTF-8` 与 `UTF-8 with BOM`
  在 Windows 上直接用记事本或 PowerShell 写文件也能正常读取。
- `output_root`
  输出根目录。
- `chromium_executable_path`
  留空使用 Playwright 默认 Chromium；填写后使用指定浏览器路径。
- `max_concurrency`
  单站点并发页面数。
- `max_site_concurrency`
  站点级并发任务数；服务器上建议大于 `1`。
- `max_pages_per_site`
  单站点最大访问页数，`0` 表示不限制。
- `write_full_outputs_on_checkpoint`
  `true` 时每次 checkpoint 都会重写 `nodes/edges/visits/all_urls` 明细；`false` 时只写 `summary.json + checkpoint.json`，适合高并发服务器。
- `visit_leaf_pages`
  `true` 表示同站点 HTML 页面默认都访问；`false` 表示保守模式。
- `enable_generic_interactions`
  是否启用通用交互点击。
- `max_interaction_clicks_per_page`
  每页最多点击多少次。
- `enable_cbpt_portal_ajax_expansion`
  是否启用 `cbpt portal` 页面的 AJAX 分页展开。
- `max_cbpt_portal_ajax_requests_per_page`
  每个 `cbpt portal` 页面最多额外展开多少个 AJAX 分页请求。
- `proxy_servers`
  代理池，可为字符串列表，也可为对象列表。
- `proxy_session_count`
  每个站点启用多少个代理会话。
- `skip_failed_proxies`
  坏代理是否自动跳过。
- `browser_launch_args`
  Chromium 启动参数，Docker 环境下通常会补 `--no-sandbox`。
- `enable_request_blocking`
  是否屏蔽图片、字体、媒体等低价值资源。

### `proxy_servers` 配置示例

```json
{
  "proxy_servers": [
    "http://124.71.126.1:8883",
    "http://124.71.126.1:8887",
    {
      "server": "http://124.71.126.2:8883",
      "username": "",
      "password": "",
      "label": "proxy-02-8883"
    }
  ]
}
```

### `chromium_executable_path` 配置示例

```json
{
  "chromium_executable_path": "C:\\Program Files\\Chromium\\Application\\chrome.exe"
}
```

## 输入文件

默认输入文件：[input_urls.txt](/D:/Desktop/qoder%20work/input_urls.txt)

示例：

```text
https://zgncjj.ajcass.com/#/
https://erj.ajcass.com/#/index
https://jjgl.ajcass.com/
https://gggl.cbpt.cnki.net/portal
```

规则：

- 同一站点如果在输入文件中出现多次，只会生成一个站点任务。
- 如果某个站点之前已经 `completed = true`，但你后来又往 `input_urls.txt` 里增加了新的同站点 seed，程序会自动恢复这个站点任务并合并新 seed，不会因为旧 checkpoint 而整站跳过。
- 站点目录名按 host 转为下划线格式。
- 例如 `www.baidu.com` 会输出到 `www_baidu_com/`。

## 运行

### 本地运行

```powershell
& 'C:\Python314\python.exe' ajcass_crawler.py
```

或：

```powershell
& 'C:\Python314\python.exe' site_batch_crawler.py
```

### 服务器运行

```bash
python3 server_batch_crawler.py
```

## 断点续传

每个站点目录下都会生成 `checkpoint.json`。

行为说明：

- 程序中断后再次运行，会从检查点继续。
- 已访问 URL 不会重复入队。
- 已完成站点默认会跳过。
- 但如果同一站点新增了 seed URL，即使旧 checkpoint 已完成，也会自动恢复该站点并把新 seed 并入同一个站点目录继续挖掘。
- 当爬取策略版本升级后，旧检查点会自动识别为“需要继续补跑”。
- 正在访问中的页面也会写入 checkpoint；异常退出后会重新入队，不会因为调度中的页面丢失覆盖。
- 无论是否启用轻量 checkpoint，都会持续追加：
  - `all_discovered_urls.live.txt`
  - `all_discovered_urls.live.tsv`
  用来实时查看“到目前为止新发现了哪些 URL”。

## 输出结构

```text
crawl_output_batch/
  batch.log
  all_discovered_urls.txt
  all_discovered_urls.tsv
  all_discovered_urls.csv
  batch_summary.json
  sites_summary.csv
  gggl_cbpt_cnki_net/
    all_discovered_urls.txt
    all_discovered_urls.live.txt
    all_discovered_urls.live.tsv
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

- 根目录 `all_discovered_urls.*` 是全局汇总。
- `sites_summary.csv` 是按站点的汇总表。
- 站点目录下的 `nodes.*` / `edges.*` / `visits.*` 是单站点明细。
- 如果 `write_full_outputs_on_checkpoint = false`，运行中途这些明细文件可能不是最新状态；站点本轮结束时会统一刷新完整明细。
- 运行中想看进度时，优先看：
  - `summary.json`
  - `all_discovered_urls.live.txt`
  - `all_discovered_urls.live.tsv`

## 日志

日志分两层：

- 根目录 `batch.log`
  记录批任务启动、站点开始/结束、全局汇总输出。
- 站点目录 `crawl.log`
  记录页面访问、检查点、发现数量、异常堆栈。

建议：

- 日常跑批：`log_level = "INFO"`
- 深入排查：`log_level = "DEBUG"`

## 本次通用站点验证

2026 年 4 月 26 日，我用本地 `Python 3.14 + Playwright` 对下面三个站点做了限额回归：

- `http://www.cpedm.com`
- `http://www.cssm.com.cn`
- `https://zgncjj.ajcass.com/#/`

在 `max_pages_per_site = 6`、`write_full_outputs_on_checkpoint = true` 的 smoke 配置下，结果是：

- `cpedm`：`777` discovered，`333` queueable，`6` visited
- `cssm`：`254` discovered，`209` queueable，`6` visited
- `zgncjj`：`452` discovered，`340` queueable，`6` visited

这轮重点验证了三件事：

- `cpedm`
  已确认会从首页进入 `CN/1000-0747/home.shtml`，并优先继续下钻到 `CN/Y2000/V27/I1`、`CN/Y2000/V27/I2` 这类期次页，而不是先被 `articleDownloadControl`、点击量统计接口抢占预算。
- `cssm`
  已确认会优先访问 `/ch/index.aspx`、`/ch/reader/current.aspx`、`more_news_list.aspx`、`view_news.aspx`，而不是优先跑 `css.aspx` 之类低价值页面。
- `zgncjj`
  已确认本地依然能稳定发现大量 `#/search?...` 与 `#/issueDetail?...`，并继续通过 AJCASS 接口向下展开；同时已经压掉此前那类 `https://zgncjj.ajcass.com/span`、`/em`、`/li` 这类伪 URL 噪声。

## 本次 `gggl` 本地验证

本地使用 `Python 3.14 + Playwright` 做了多轮回归，重点验证的是 `https://gggl.cbpt.cnki.net/portal` 这类 `cbpt portal` 站点。

验证结论：

- 首页 `portal` 可以直接展开出大量 `guokan_list` 期次入口。
- `paper_list/type_benqi`、`guokan_list?...` 这类论文索引页已能继续向下发现大量论文详情 URL。
- `onclick` 中的下载、新闻、友情链接、CNKI 跳转 API 已能被抽取。
- `paperPage_list` / `listPrePaperOrNextPaper` 这类 AJAX 分页已能被主动展开，而不是只靠页面 URL 变化。
- 本地做过 20 页、60 页、180 页三档回归；在较大的快速回归里，`portal` 首页、过刊页、论文列表页和文章页都已经能稳定串起来。

最新一次轻量 smoke 输出示例：

- [summary.json](/D:/Desktop/qoder%20work/crawl_output_gggl_test/gggl_cbpt_cnki_net/summary.json)
- [nodes.csv](/D:/Desktop/qoder%20work/crawl_output_gggl_test/gggl_cbpt_cnki_net/nodes.csv)
- [crawl.log](/D:/Desktop/qoder%20work/crawl_output_gggl_test/gggl_cbpt_cnki_net/crawl.log)

说明：

- 轻量回归时为了控制时间，给 `gggl` 加了页面上限，所以 `completed = false` 属于预期。
- 真实跑批时把 `max_pages_per_site` 调大或设为 `0` 即可继续补跑。

## 说明

- 代码仓库默认公开配置结构，不对字段做隐藏处理。
- 当前仍会记录部分站点内部的低价值辅助 URL，但它们不会重复入队，也不会影响断点续传。

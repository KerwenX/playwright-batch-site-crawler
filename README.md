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

## 设计目标

默认策略不是“识别正文页后停止”，而是：

- 只要是同站点、可访问、像 HTML 的 URL，就入队并访问一次。
- 外站 URL、下载资源、明显危险动作 URL 只记录，不继续访问。
- 已访问 URL 不会重复入队，避免死循环。
- 页面存在交互、弹窗、AJAX 分页时，会尽量展开后继续发现 URL。

## 已适配的站点类型

### 1. 通用站点

- 普通多页站
- 基于渲染后的 DOM 抽取链接
- 通用按钮、分页、Tab、More 链接点击探测

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

## 配置

本地默认配置文件：[config.json](/D:/Desktop/qoder%20work/config.json)

服务器示例配置文件：[config.server.example.json](/D:/Desktop/qoder%20work/config.server.example.json)

常用字段：

- `input_urls_file`
  批量种子 URL 文件，一行一个 URL。
- `output_root`
  输出根目录。
- `chromium_executable_path`
  留空使用 Playwright 默认 Chromium；填写后使用指定浏览器路径。
- `max_concurrency`
  单站点并发页面数。
- `max_pages_per_site`
  单站点最大访问页数，`0` 表示不限制。
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
- 当爬取策略版本升级后，旧检查点会自动识别为“需要继续补跑”。

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

## 日志

日志分两层：

- 根目录 `batch.log`
  记录批任务启动、站点开始/结束、全局汇总输出。
- 站点目录 `crawl.log`
  记录页面访问、检查点、发现数量、异常堆栈。

建议：

- 日常跑批：`log_level = "INFO"`
- 深入排查：`log_level = "DEBUG"`

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

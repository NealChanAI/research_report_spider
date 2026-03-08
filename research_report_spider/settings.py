# -*- coding: utf-8 -*-

# Scrapy settings for research_report_spider project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'research_report_spider'

SPIDER_MODULES = ['research_report_spider.spiders']
NEWSPIDER_MODULE = 'research_report_spider.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'research_report_spider (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'research_report_spider.middlewares.ResearchReportSpiderSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'research_report_spider.middlewares.ResearchReportSpiderDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
# 默认关闭写入 MySQL 的管道；PDF 下载管道已启用，将研报 PDF 保存到本地。
# 如需启用数据库写入，在 ITEM_PIPELINES 中取消 MysqlPipeline 注释即可。
ITEM_PIPELINES = {
    'research_report_spider.pipelines.MyFilesPipeline': 200,
    # 'research_report_spider.pipelines.MysqlPipeline': 300,
}

# PDF 下载保存根目录（相对项目根目录）；可改为绝对路径如 "H-hezudao/Research_Report"
FILES_STORE = "pdf_downloads"

# 将抓取结果导出为 CSV 文件
# 文件名含触发时间戳，如 reports_20260304_153045.csv，避免多次运行互相覆盖
import os
from datetime import datetime

_feed_ts = os.environ.get("REPORT_FEED_TIMESTAMP") or datetime.now().strftime("%Y%m%d_%H%M%S")
FEEDS = {
    f"reports_{_feed_ts}.csv": {
        "format": "csv",
        "encoding": "utf-8-sig",  # 方便在 Excel 中打开不乱码
        "overwrite": True,        # 单次运行内覆盖同名文件
    },
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

mysql_host = '127.0.0.1'
mysql_user = 'root'
mysql_password = 'root'
mysql_db = 'crawl'
mysql_table = 'research_report'

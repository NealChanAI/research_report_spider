# -*- coding: utf-8 -*-
import scrapy
import datetime
import logging
import json

from selenium import webdriver

from research_report_spider.common import operation
from research_report_spider.items import ResearchReportSpiderItem


def get_csi500_codes():
    """获取中证500成分股代码列表（通过 akshare）"""
    try:
        import akshare as ak
        df = ak.index_stock_cons_csindex(symbol="000905")
        codes = df["成分券代码"].astype(str).str.split(".").str[0].tolist()
        return ",".join(codes)
    except Exception as e:
        logging.error("获取中证500成分股失败: %s，请确保已安装 akshare: pip install akshare", e)
        raise


class ReportSpider(scrapy.Spider):
    name = 'report'
    allowed_domains = ['gw.datayes.com']
    start_urls = ['http://gw.datayes.com/']

    # 默认：今天日期
    dt = datetime.datetime.now().strftime('%Y-%m-%d')
    today = dt.replace('-', '')

    base_url = 'https://gw.datayes.com/rrp_adventure/web/search?'
    headers = {
        "Origin": "https://robo.datayes.com",
        "Referer": "https://robo.datayes.com/v2/fastreport/company?subType=%E4%B8%8D%E9%99%90&induName=",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36",
    }
    # pageNow, pubTimeStart, pubTimeEnd, secCodeList
    url = "https://gw.datayes.com/rrp_adventure/web/search?pageNow={0}&authorId=&isOptional=false&orgName=&reportType=COMPANY&secCodeList={3}&reportSubType=&industry=&ratingType=&pubTimeStart={1}&pubTimeEnd={2}&type=EXTERNAL_REPORT&pageSize=40&sortOrder=desc&query=&minPageCount=&maxPageCount="

    def __init__(self, sec_codes=None, date=None, start_date=None, end_date=None, pages=4,
                 index=None, batch_size=50, *args, **kwargs):
        """
        可通过命令行参数指定爬取范围：
        - sec_codes: 股票代码，逗号分隔。不传则爬取全部
        - date: 单日，格式 YYYYMMDD。与 start_date/end_date 二选一
        - start_date, end_date: 日期范围，格式 YYYYMMDD。用于爬取多天
        - pages: 爬取页数，默认 4。大范围爬取时会自动分页直到无数据
        - index: 指数代码，如 csi500 自动获取中证500成分股
        - batch_size: 股票分批大小，默认 50（500 只股票分 10 批）
        """
        super().__init__(*args, **kwargs)
        self.batch_size = int(batch_size) if batch_size else 50

        # 股票代码
        if index and index.lower() == "csi500":
            self.sec_code_batches = self._batch_codes(get_csi500_codes())
            logging.info("使用中证500成分股，共 %d 批", len(self.sec_code_batches))
        elif sec_codes:
            self.sec_code_batches = self._batch_codes((sec_codes or "").strip())
        else:
            self.sec_code_batches = [""]

        # 日期范围
        if start_date and end_date:
            self.start_date = str(start_date).replace("-", "")
            self.end_date = str(end_date).replace("-", "")
        elif date:
            d = str(date).replace("-", "")
            self.start_date = self.end_date = d
        else:
            self.start_date = self.end_date = self.today

        self.pages = int(pages) if pages else 4
        self.auto_paginate = bool(start_date and end_date) or len(self.sec_code_batches) > 1

    def _batch_codes(self, codes_str):
        """将股票代码字符串按 batch_size 分批"""
        if not codes_str:
            return [""]
        codes = [c.strip() for c in codes_str.split(",") if c.strip()]
        batches = []
        for i in range(0, len(codes), self.batch_size):
            batches.append(",".join(codes[i:i + self.batch_size]))
        return batches

    def start_requests(self):
        cookie = self.get_cookies()

        for sec_batch in self.sec_code_batches:
            if self.auto_paginate:
                # 大范围爬取：只请求第 1 页，parse 中按需请求后续页
                pages_to_request = [1]
            else:
                pages_to_request = range(1, self.pages + 1)

            for page in pages_to_request:
                yield scrapy.Request(
                    self.url.format(page, self.start_date, self.end_date, sec_batch),
                    headers=self.headers,
                    cookies=cookie,
                    meta={
                        "page": page,
                        "cookie": cookie,
                        "sec_batch": sec_batch,
                        "auto_paginate": self.auto_paginate,
                    },
                )

    def _build_next_page_request(self, response, next_page):
        """构造下一页请求"""
        sec_batch = response.meta.get("sec_batch", "")
        return scrapy.Request(
            self.url.format(next_page, self.start_date, self.end_date, sec_batch),
            headers=self.headers,
            cookies=response.meta.get("cookie", {}),
            meta={
                "page": next_page,
                "cookie": response.meta.get("cookie"),
                "sec_batch": sec_batch,
                "auto_paginate": True,
            },
            callback=self.parse,
        )

    def parse(self, response):
        page = response.meta.get('page')
        logging.info('正在抓取第{0}页'.format(page))
        result = json.loads(response.text)

        message = result['message']
        if message != 'success':
            logging.info('message为：{0},请求失败！'.format(message))
            return

        data_all = result['data']['list']

        # 大范围爬取时：若本页满 40 条则继续请求下一页
        if response.meta.get("auto_paginate") and len(data_all) >= 40:
            yield self._build_next_page_request(response, page + 1)
        for info in data_all:
            data = info['data']
            report_id = data['id']
            stock_name = data['companyName']
            author = data['author']
            title = data['title']
            # id判断
            is_ar_id = operation.get_article_id(report_id)
            if is_ar_id:
                logging.info('id:{0}已经存在'.format(is_ar_id))
                continue

            content = data['abstractText']
            if content:
                content = content.replace('\u3000', '').strip()
            else:
                content = content
            stock_code_info = data['stockInfo']
            if stock_code_info is None:
                stock_code = None
            else:
                stock_code = stock_code_info['stockId']

            file_name = '{0}-{1}.pdf'.format(stock_code, title)
            org_dt = data['publishTime'].split('T')
            publish_time = org_dt[0]

            keys = publish_time.split('-')
            year = keys[0]

            filename = "/{0}/{1}/{2}".format(year, publish_time, file_name)

            item = ResearchReportSpiderItem()
            # 作为id，唯一主键
            item['report_id'] = report_id
            # 股票代码
            item['stock_code'] = stock_code
            # 股票名称
            item['stock_name'] = stock_name
            # 日期
            item['publish_time'] = publish_time
            # 作者
            item['author'] = author
            # 研报标题
            item['title'] = title
            # 原文评级
            item['original_rating'] = data['ratingContent']
            # 评级变动
            item['rating_changes'] = data['ratingType']
            #
            item['rating_adjust_mark_type'] = data['ratingAdjustMarkType']
            #  机构
            item['org_name'] = data['orgName']
            # 内容
            item['content'] = content
            # pdf链接
            item['pdf_link'] = [data['s3Url']]
            # 文件名
            item['filename'] = filename
            # 文件存储路径
            item['save_path'] = "H-hezudao/Research_Report{0}".format(filename)

            yield item

    def get_cookies(self):
        # Chrome 无头浏览器模式（Chrome 在 Windows 上更常见，Selenium 支持更好）
        from selenium.webdriver.chrome.options import Options
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=chrome_options)

        url = 'https://robo.datayes.com/v2/fastreport/company?subType=%E4%B8%8D%E9%99%90&induName='
        driver.get(url)
        # 获取cookie列表
        cookie_list = driver.get_cookies()
        # 格式化打印cookie
        cookie_dict = {}
        for cookie in cookie_list:
            cookie_dict[cookie['name']] = cookie['value']
        driver.quit()
        logging.info('Chrome 已经 quit')
        return cookie_dict

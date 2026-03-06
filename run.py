# -*- coding: utf-8 -*-
"""
运行研报爬虫。

用法示例：
  python run.py                                    # 默认：今天、4页、全部股票
  python run.py -s 002003,600570                   # 指定股票代码
  python run.py -d 20260301                         # 指定日期
  python run.py --index csi500 --months 3          # 中证500 最近3个月（推荐）
  python run.py --index csi500 --start 20251204 --end 20260304   # 或指定日期范围
"""
import os
import argparse
from datetime import datetime, timedelta
from scrapy import cmdline

file_log = os.getcwd() + "/info.log"
if os.path.exists(file_log):
    os.remove(file_log)
    print("每次运行前把之前的日志文件删除,保留最新日志即可")

parser = argparse.ArgumentParser(description='研报爬虫')
parser.add_argument('-s', '--sec-codes', help='股票代码，逗号分隔，如 002003,600570')
parser.add_argument('-d', '--date', help='单日，格式 YYYYMMDD')
parser.add_argument('--start', help='开始日期 YYYYMMDD，与 --end 配合用于日期范围')
parser.add_argument('--end', help='结束日期 YYYYMMDD')
parser.add_argument('--months', type=int, help='最近 N 个月，与 --index 配合使用，如 --index csi500 --months 3')
parser.add_argument('-p', '--pages', type=int, default=4, help='爬取页数，默认 4（大范围时自动分页）')
parser.add_argument('--index', choices=['csi500'], help='指数：csi500=中证500成分股')
parser.add_argument('--batch-size', type=int, default=50, help='股票分批大小，默认 50')
parser.add_argument('--delay', type=float, default=None, help='请求间隔秒数，大范围爬取建议 5-8')
args = parser.parse_args()

# --months 自动计算日期范围
if args.months:
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=args.months * 30)
    args.start = start_dt.strftime("%Y%m%d")
    args.end = end_dt.strftime("%Y%m%d")
    print(f"日期范围: {args.start} ~ {args.end} (最近 {args.months} 个月)")

# 大范围爬取（中证500 或 日期范围）时默认 5 秒间隔，降低被封风险
if args.delay is None and (args.index or (args.start and args.end)):
    args.delay = 5
    print("大范围爬取，已自动设置请求间隔为 5 秒（可用 --delay 覆盖）")

# 触发时间戳，用于导出文件名（格式 YYYYMMDD_HHMMSS，兼容 Windows）
feed_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
os.environ["REPORT_FEED_TIMESTAMP"] = feed_ts
print(f"导出文件: reports_{feed_ts}.csv")

cmd = ['scrapy', 'crawl', 'report', '-a', f'pages={args.pages}', '-a', f'batch_size={args.batch_size}']
if args.sec_codes:
    cmd.extend(['-a', f'sec_codes={args.sec_codes}'])
if args.date:
    cmd.extend(['-a', f'date={args.date}'])
if args.start:
    cmd.extend(['-a', f'start_date={args.start}'])
if args.end:
    cmd.extend(['-a', f'end_date={args.end}'])
if args.index:
    cmd.extend(['-a', f'index={args.index}'])
if args.delay is not None:
    cmd.extend(['-s', f'DOWNLOAD_DELAY={args.delay}'])

cmdline.execute(cmd)

# -*- coding: utf-8 -*-
"""检查研报 content 字段空值率"""

import csv
import sys
import io
import glob
import os

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# content 在表头中的列索引（author,content,filename,...）
CONTENT_IDX = 1

def check_file(path):
    total = 0
    empty = 0
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            total += 1
            if len(row) <= CONTENT_IDX or not (row[CONTENT_IDX] or "").strip():
                empty += 1
    return total, empty

def main():
    # 优先检查带时间戳的最新文件
    files = sorted(glob.glob("reports_*.csv"), reverse=True)
    if not files:
        files = ["reports.csv"] if os.path.exists("reports.csv") else []
    
    if not files:
        print("未找到 reports*.csv 文件，请先运行爬虫")
        return

    print("=" * 50)
    print("研报 content 空值率检查")
    print("=" * 50)

    for path in files:
        if not os.path.exists(path):
            continue
        total, empty = check_file(path)
        if total == 0:
            rate = 0.0
        else:
            rate = empty / total * 100
        print(f"\n文件: {path}")
        print(f"  总条数: {total}")
        print(f"  content 为空: {empty} 条")
        print(f"  空值率: {rate:.2f}%")

    print("\n" + "=" * 50)

if __name__ == "__main__":
    main()

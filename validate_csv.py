# -*- coding: utf-8 -*-
"""验证 reports.csv 数据完整性：检查串行、缺失、格式问题"""

import csv
import sys
import io

# Windows 控制台 UTF-8 输出
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from collections import Counter, defaultdict

CSV_PATH = "reports.csv"
EXPECTED_COLS = 14  # author,content,filename,org_name,original_rating,pdf_link,publish_time,rating_adjust_mark_type,rating_changes,report_id,save_path,stock_code,stock_name,title

def main():
    issues = []
    row_count = 0
    col_counts = Counter()
    report_ids = set()
    dup_ids = []
    empty_critical = defaultdict(list)  # field -> [row indices]
    date_range = {"min": None, "max": None}
    stock_codes = set()

    with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        if len(header) != EXPECTED_COLS:
            issues.append(f"表头列数异常: {len(header)} (期望 {EXPECTED_COLS})")
            print(f"表头: {header}")

        for i, row in enumerate(reader):
            row_count += 1
            col_counts[len(row)] += 1

            if len(row) != EXPECTED_COLS:
                issues.append(f"第 {i+2} 行列数异常: {len(row)} (期望 {EXPECTED_COLS})")
                if len(row) > EXPECTED_COLS:
                    issues.append(f"  -> 可能 content 字段内逗号未正确转义导致串行")
                elif len(row) < EXPECTED_COLS:
                    issues.append(f"  -> 可能字段缺失或截断")

            # 按列索引检查关键字段 (基于表头顺序)
            # author(0), content(1), filename(2), org_name(3), original_rating(4), pdf_link(5),
            # publish_time(6), rating_adjust_mark_type(7), rating_changes(8), report_id(9),
            # save_path(10), stock_code(11), stock_name(12), title(13)
            if len(row) >= EXPECTED_COLS:
                report_id = row[9].strip() if len(row) > 9 else ""
                if report_id:
                    if report_id in report_ids:
                        dup_ids.append((i + 2, report_id))
                    report_ids.add(report_id)

                # 关键字段非空检查
                critical = {
                    "report_id": 9,
                    "stock_code": 11,
                    "title": 13,
                    "publish_time": 6,
                }
                for name, idx in critical.items():
                    if idx < len(row) and (not row[idx] or not row[idx].strip()):
                        empty_critical[name].append(i + 2)

                # 日期范围
                if len(row) > 6 and row[6]:
                    d = row[6].strip()
                    if d and len(d) >= 10:
                        if date_range["min"] is None or d < date_range["min"]:
                            date_range["min"] = d
                        if date_range["max"] is None or d > date_range["max"]:
                            date_range["max"] = d

                if len(row) > 11 and row[11]:
                    stock_codes.add(row[11].strip())

    # 汇总报告
    print("=" * 60)
    print("reports.csv 数据完整性验证报告")
    print("=" * 60)
    print(f"总行数: {row_count}")
    print(f"表头列数: {len(header)} (期望 {EXPECTED_COLS})")
    print()

    # 列数分布
    print("列数分布:")
    for cols, cnt in sorted(col_counts.items()):
        status = "[OK]" if cols == EXPECTED_COLS else "[!!]"
        print(f"  {status} {cols} 列: {cnt} 行")
    print()

    # 串行/格式问题
    if any(c != EXPECTED_COLS for c in col_counts):
        print("【串行/格式问题】")
        for iss in issues[:20]:  # 最多显示20条
            print(f"  {iss}")
        if len(issues) > 20:
            print(f"  ... 共 {len(issues)} 条异常")
        print()
    else:
        print("【串行检查】[OK] 所有行列数一致，无串行")
        print()

    # 重复 report_id
    if dup_ids:
        print("【重复 report_id】")
        for line, rid in dup_ids[:10]:
            print(f"  第 {line} 行: {rid}")
        if len(dup_ids) > 10:
            print(f"  ... 共 {len(dup_ids)} 条重复")
        print()
    else:
        print("【重复检查】[OK] 无重复 report_id")
        print()

    # 关键字段空值
    if empty_critical:
        print("【关键字段空值】")
        for name, rows in empty_critical.items():
            print(f"  {name} 为空: {len(rows)} 行 (示例行号: {rows[:5]})")
        print()
    else:
        print("【空值检查】[OK] 关键字段无空值")
        print()

    # 日期范围
    if date_range["min"] and date_range["max"]:
        print("【日期范围】")
        print(f"  最早: {date_range['min']}")
        print(f"  最晚: {date_range['max']}")
        print()

    # 股票覆盖
    print("【股票覆盖】")
    print(f"  涉及股票数: {len(stock_codes)}")
    print()

    # 结论
    print("=" * 60)
    has_error = issues or dup_ids or empty_critical
    if not has_error:
        print("结论: 数据完整，无串行或明显缺失")
    else:
        print("结论: 存在上述问题，建议检查爬虫或数据源")
    print("=" * 60)

if __name__ == "__main__":
    main()

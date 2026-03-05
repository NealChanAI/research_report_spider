# -*- coding: utf-8 -*-
import pymysql

from research_report_spider.settings import (
    mysql_host,
    mysql_user,
    mysql_password,
    mysql_db,
    mysql_table,
)


# 为了在本地没有 MySQL 服务时也能运行爬虫，这里把连接失败的异常吞掉，
# 只是在去重时返回 None，不再中断整个项目。
try:
    conn = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        passwd=mysql_password,
        db=mysql_db,
        charset="utf8",
        use_unicode=True,
    )
    cursor = conn.cursor()
except pymysql.Error as e:
    # 打印错误信息，但不让异常向外抛出
    print("MySQL 连接失败（operation.py）：", e)
    conn = None
    cursor = None


def get_article_id(art_id):
    """验证 article_id 是否已存在；如果数据库不可用，直接返回 None"""
    # 如果没有成功连接数据库，直接认为不存在，避免阻塞爬虫
    if cursor is None:
        return None

    try:
        sql = "select * from {0} where report_id=%s;".format(mysql_table)
        cursor.execute(sql, (art_id,))
        results = cursor.fetchall()
        if results:
            return results[0][0]
        else:
            return None
    except pymysql.Error as e:
        print(e)
        return None
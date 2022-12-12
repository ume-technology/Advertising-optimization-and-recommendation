# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:functions.py
@Time:2022/12/7 10:01
@Read: 
"""
from odps import ODPS
from odps import options

ACCESS_ID = 'aaaaaf8v5iivK9FoBk'
ACCESS_KEY = 'aaaaaHQbVDSpJB3xvULi85h0TJxYzo'
DEFAULT_PROJECT = 'cda'
END_POINT = 'http://service.cn-shenzhen.maxcompute.aliyun.com/api'

o = ODPS(ACCESS_ID, ACCESS_KEY, DEFAULT_PROJECT, endpoint=END_POINT)
options.tunnel.use_instance_tunnel = True
options.tunnel.limit_instance_tunnel = False  # 关闭limit限制，读取全部数据

# with o.execute_sql(sql).open_reader(tunnel=True) as reader:
#     all_top_apt = reader.to_pandas()


# todo 链接数据库的两种方式
# import pymysql
# import pandas as pd
# host = 'ro.hwaurora.rdsdb.com'
# port = 3306
# db = 'gdsc'
# user = 'fanzhimin'
# passwd = 'x%HpQhq8m9c#5zx@'
# conn = pymysql.connect(host=host, port=port, user=user, db=db, password=passwd, charset='utf8')
# # todo cursor
# # cursor = conn.cursor()
# # count_ = cursor.execute(sql)  # 返回的是查询到的数据库的数据条目数量
# # print(count_)
# # data = cursor.fetchall()
# # todo pandas
# dataDF = pd.read_sql(sql, conn)
# return dataDF
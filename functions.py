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

ACCESS_ID = 'aaaaaa9Sf8v5iivK9FoBk'
ACCESS_KEY = 'aaaaaaDSpJB3xvULi85h0TJxYzo'
DEFAULT_PROJECT = 'cda'
END_POINT = 'http://service.cn-shenzhen.maxcompute.aliyun.com/api'

o = ODPS(ACCESS_ID, ACCESS_KEY, DEFAULT_PROJECT, endpoint=END_POINT)
options.tunnel.use_instance_tunnel = True
options.tunnel.limit_instance_tunnel = False  # 关闭limit限制，读取全部数据

# with o.execute_sql(sql).open_reader(tunnel=True) as reader:
#     all_top_apt = reader.to_pandas()

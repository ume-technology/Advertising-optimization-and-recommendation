# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@Blog: https://www.umeai.top/
@File:read_tb_dim_pro_gk_product_df.py
@Time:2022/11/15 14:42
@ReadMe: 读取tb_dim_pro_gk_product_df 表中的产品的三级分类信息；这是产品维表信息
"""
import pickle
from odps import ODPS
from odps import options

ACCESS_ID = 'LTAI5t6MruTBLbd9GPYaef7B'
ACCESS_KEY = 'ph8zkHlOitQkn6sIBKTgiXDPx4L6gF'
DEFAULT_PROJECT = 'cda'
END_POINT = 'http://service.cn-shenzhen.maxcompute.aliyun.com/api'
o = ODPS(ACCESS_ID, ACCESS_KEY, DEFAULT_PROJECT, endpoint=END_POINT)
options.tunnel.use_instance_tunnel = True
options.tunnel.limit_instance_tunnel = False  # 关闭limit限制，读取全部数据


def run_sql(sql):
    with o.execute_sql(sql).open_reader(tunnel=True) as reader:
        df = reader.to_pandas()
    # with open('tb_dim_pro_gk_product_df.pick', 'wb') as f:
    #     pickle.dump(df, f)


if __name__ == '__main__':
    sql = 'select * from giikin_aliyun.tb_dim_pro_gk_product_df'
    # run_sql(sql)

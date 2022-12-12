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
from functions import *


def run_sql(sql):
    with o.execute_sql(sql).open_reader(tunnel=True) as reader:
        df = reader.to_pandas()
    # with open('tb_dim_pro_gk_product_df.pick', 'wb') as f:
    #     pickle.dump(df, f)


if __name__ == '__main__':
    sql = 'select * from giikin_aliyun.tb_dim_pro_gk_product_df'
    # run_sql(sql)

# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:readdatafrommatterial.py
@Time:2022/12/9 10:12
@Read: 读取广告素材信息; todo ner and other task
"""
import pickle
from functions import *


# todo Zero：这是最开始的读取原始 AD mat table 中的数据；这一步执行完毕，会生成readadtextdatabase.pick文件，以被First步骤使用；在First中，有清洗工作，还有OpenKG知识融合的工作；
def run_sql(sql):
    with o.execute_sql(sql).open_reader(tunnel=True) as reader:
        df = reader.to_pandas()
    with open('./readadtextdatabase.pick', 'wb') as f:
        pickle.dump(df, f)


# if __name__ == '__main__':
#     demo = """SELECT order_id,order_price from giikin_aliyun.tb_dwd_ord_gk_order_info_crt_df WHERE pt>='20220301'"""
#
#     sql_less = """ select  --     # run this sql
#         platfrom,team_name,line_name,
#         designer_id,opt_id,
#         product_id,product_name,sale_id,sale_name,
#         order_cnt,clicks_cnt,clicks_rate,add_cart_cnt,checkout_cnt,conversions_rate,people_cover_cnt,age_range,country,genders,currency_id,lang_id,
#         ad_slogans,interest_words,ad_account_id,campaign_id,ad_group_id
#         from giikin_aliyun.tb_rp_mar_ad_material_df where pt='20221007' and (lang_id in (1, 2) or currency_id in (3,13,11));"""
#     run_sql(sql_less)


with open('./readadtextdatabase.pick', 'rb') as f:
    original_ad_text = pickle.load(f)

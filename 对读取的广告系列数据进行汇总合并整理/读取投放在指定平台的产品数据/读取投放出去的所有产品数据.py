# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:读取投放出去的所有产品数据.py
@Time:2022/11/29 16:29
@Read: 这部分的数据来自脚本read_gdsc_gk_product_report.py：在这个读取数据库的原始过程中，数据产品的平台做了限制；
       这里只是做上述数据的加载；因此这里加载出来的产品数据是指定平台的；因为目前处理的是FB品台的优化师，因此这里就是FB平台的数据；
"""
import pickle

# todo 接下来做的是把好的优化师投放的商品选择了出来; 这里的产品数据是只有投放在FB的产品
# todo load gk_product_reporter table data  从这里可以看出来每个优化师投放的商品数据的report - 只有 FB 平台的产品数据
with open('/对读取的广告系列数据进行汇总合并整理/读取投放在指定平台的产品数据/gk_product_report_fb.pick', 'rb') as f:
    gk_product_reporter_df = pickle.load(f)
    # columns_product_table = gk_product_reporter_df.columns
    ader_id_groups = gk_product_reporter_df.groupby('ader_id').groups
    # ader_id_list = ader_id_groups.keys()

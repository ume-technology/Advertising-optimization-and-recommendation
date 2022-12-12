# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:EachHomeTop10ByMonth_11.py
@Time:2022/11/26 15:23
@Read: 基于该数据统计的基础思路：优化师/广告/产品
        1.首先读取了各个家族的Top优化师s的ID数据；（这里有一个依赖是每个月各团队的Top优化师s可能会不同，因此要注意月份）
        2.然后读取了所有投放在FB平台的产品，匹配了这些优化师投放在FB平台的都有哪些产品（无月份依赖）；
        3.最后读取了这些Top优化师投放出来的广告系列都有哪些（无月份依赖）；
"""
# # 获取优化师的信息
from 优化师_高度汇总表_tb_dws_ord_order_si_crt_df.EachHomeTop10ByMonth import all_top_apt
# 获取优化师投放的广告涉及的商品信息
from 优化师_投放的广告涉及的产品信息_gdsc.read_gdsc_gk_product_report import gk_product_reporter_df, ader_id_groups

# todo 确定优化师投放的商品
# 202211月份 - 筛选特定家族的优化师
火凤凰Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "火凤凰家族"]['opt_id'].to_list()
神龙Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "神龙家族"]['opt_id'].to_list()
精灵Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "精灵家族"]['opt_id'].to_list()
红杉Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "红杉家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好
金牛Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "金牛家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好
金狮Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "金狮家族"]['opt_id'].to_list()
金蝉Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "金蝉家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好
雪豹Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "雪豹家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好
飞马Top优化师sID_11 = all_top_apt.loc[all_top_apt["family_name"] == "飞马家族"]['opt_id'].to_list()  # 11月份相对没有其它家族好

# todo 接下来做的是把好的优化师投放的商品以及商品的report选择了出来;
# wait 但是这里没有完成把当前月度的下优化师投放的商品删选出来，这里只是匹配到了这个优化师投放的所有商品
火凤凰Top优化师sID_11_product = {}
for eachOptId in 火凤凰Top优化师sID_11:
    lineIndex = ader_id_groups.get(str(eachOptId))
    火凤凰Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]
    break
# 神龙Top优化师sID_11_product = {}
# for eachOptId in 神龙Top优化师sID_11:
#     lineIndex = ader_id_groups.get(str(eachOptId))
#     神龙Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]
# 精灵Top优化师sID_11_product = {}
# for eachOptId in 精灵Top优化师sID_11:
#     lineIndex = ader_id_groups.get(str(eachOptId))
#     精灵Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]
# 红杉Top优化师sID_11_product = {}
# for eachOptId in 红杉Top优化师sID_11:
#     lineIndex = ader_id_groups.get(str(eachOptId))
#     红杉Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]
# 金牛Top优化师sID_11_product = {}
# for eachOptId in 金牛Top优化师sID_11:
#     lineIndex = ader_id_groups.get(str(eachOptId))
#     金牛Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]
# 金狮Top优化师sID_11_product = {}
# for eachOptId in 金狮Top优化师sID_11:
#     lineIndex = ader_id_groups.get(str(eachOptId))
#     金狮Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]
# 金蝉Top优化师sID_11_product = {}
# for eachOptId in 金蝉Top优化师sID_11:
#     lineIndex = ader_id_groups.get(str(eachOptId))
#     金蝉Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]
# 雪豹Top优化师sID_11_product = {}
# for eachOptId in 雪豹Top优化师sID_11:
#     lineIndex = ader_id_groups.get(str(eachOptId))
#     雪豹Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]
# 飞马Top优化师sID_11_product = {}
# for eachOptId in 飞马Top优化师sID_11:
#     lineIndex = ader_id_groups.get(str(eachOptId))
#     飞马Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]

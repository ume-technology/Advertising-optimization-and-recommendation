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
from core_1_优化师_高度汇总表tb_dws_ord_order_si_crt_df.EachHomeTop10ByMonth import all_top_apt  # 每更新一个月份这个数据就要重新读取
from core_4_广告系列数据and广告系列数据日报.read_gdsc_gk_product_report import gk_product_reporter_df, ader_id_groups

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

# todo 接下来做的是把好的优化师投放的商品选择了出来;
# wait 但是这里没有完成把当前月度的下优化师投放的商品删选出来，这里只是匹配到了这个优化师投放的所有商品
火凤凰Top优化师sID_11_product = {}
for eachOptId in 火凤凰Top优化师sID_11:
    lineIndex = ader_id_groups.get(str(eachOptId))
    火凤凰Top优化师sID_11_product['allgoodsby_' + str(eachOptId)] = gk_product_reporter_df.loc[lineIndex]
    break

bre = 'break'
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

# todo 核心数据，可以用来帮助获取每个优化师投放的广告，包括TOP优化师的商品数据
# wait： 同上，这里只是读取了这个优化师创建的所有广告数据，没有确定哪些广告是这些top优化师在该月分下创建的；
# wait： 另外可能还会涉及到：这些广告关联的上述的哪些产品；同时这些帮助这些优化师成为本月度的top优化师的广告都投放了多久投放了哪些系列；这些系列之间都是如何做对照；
#        更重要的：本月这些top优化师是如何设置病管理他们的广告的。
# wait:
#   1. 广告系列的竞价金额：bid_amount字段都是None；
#   2. 广告系列的竞价策略：bid_strategy都有哪些种类；
#   3. 预算类型：budget_type 都有哪些种类；
#   4. campaign_name系列的name有什么风格：和产品/商品/素材之间有什么联系与差异
#   5. daily_budget日预算和线路/商品之间的区别；
#   6. end_time结束时间是什么意思，是不是一个广告的投放时间；投放时间和线路等之间的关系；
#   7. coll_id 站点ID和订单用户之间的关系；
#   8. 产品分类；投放地区为什么都是none； goods_id【产品ID】  goods_name【产品名称】  product_id + product_name【商品ID+商品名称】 product_type【商品类型】
#   9. lifetime_budget '总预算 或 生命周期预算' 为什么都是空
#   10. start_time 开始时间是不是广告开始投放的时间   last_spend-最后一次花费改动
#   11。target_cpa 目标出价金额都是None
#   12. type 序列类型系列目标
#      ===================
#   13. 下面的字段有什么区别：
#       下面字段以本地信息为准
#       `ad_checkout` int(11) unsigned DEFAULT '0' COMMENT '广告-添加支付信息量',
#       `e_checkout` int(11) unsigned DEFAULT '0' COMMENT '本地事件-添加支付信息量',
#       `e_purchase` int(11) unsigned DEFAULT '0' COMMENT '本地事件-购买量，或 转化量',
#       `ad_purchase` int(11) unsigned DEFAULT '0' COMMENT '广告-购买量，或转化量',
火凤凰Top优化师sID_11_cam = {}
for i in 火凤凰Top优化师sID_11:
    火凤凰Top优化师sID_11_cam['camby_' + str(i)] = context.loc[context_ader_id_groups.get(float(i))]
    break

# 神龙Top优化师sID_11_cam = {}
# for i in 神龙Top优化师sID_11:
#     神龙Top优化师sID_11_cam['camby_' + str(i)] = context.loc[context_ader_id_groups.get(float(i))]
#
# 精灵Top优化师sID_11_cam = {}
# for i in 精灵Top优化师sID_11:
#     精灵Top优化师sID_11_cam['camby_' + str(i)] = context.loc[context_ader_id_groups.get(float(i))]
#
# 红杉Top优化师sID_11_cam = {}
# for i in 红杉Top优化师sID_11:
#     红杉Top优化师sID_11_cam['camby_' + str(i)] = context.loc[context_ader_id_groups.get(float(i))]
#
# 金牛Top优化师sID_11_cam = {}
# for i in range(len(金牛Top优化师sID_11) - 1, -1, -1):
#     if float(金牛Top优化师sID_11[i]) not in context_ader_id_groups_keys_list:
#         金牛Top优化师sID_11.remove(金牛Top优化师sID_11[i])
# for i in 金牛Top优化师sID_11:
#     金牛Top优化师sID_11_cam['camby_' + str(i)] = context.loc[context_ader_id_groups.get(float(i))]
#
# 金狮Top优化师sID_11_cam = {}
# for i in 金狮Top优化师sID_11:
#     金狮Top优化师sID_11_cam['camby_' + str(i)] = context.loc[context_ader_id_groups.get(float(i))]
#
# 雪豹Top优化师sID_11_cam = {}
# for i in 雪豹Top优化师sID_11:
#     雪豹Top优化师sID_11_cam['camby_' + str(i)] = context.loc[context_ader_id_groups.get(float(i))]
#
# 飞马Top优化师sID_11_cam = {}
# for i in 飞马Top优化师sID_11:
#     飞马Top优化师sID_11_cam['camby_' + str(i)] = context.loc[context_ader_id_groups.get(float(i))]

# wait 11月份的时候金蝉家族的优化师的投放的广告没有出现在广告系列表中
# 金蝉Top优化师sID_11_cam = {}
# 金蝉Top优化师sID_11_not = []
# for i in 金蝉Top优化师sID_11:
#     if float(i) not in context_ader_id_groups_keys_list:
#         金蝉Top优化师sID_11_not.append(i)
# for i in 金蝉Top优化师sID_11:
#     金蝉Top优化师sID_11_cam['camby_' + str(i)] = context.loc[context_ader_id_groups.get(float(i))]

# todo 加载广告组：主要是为了发现优化师是如何进行广告组配置的；

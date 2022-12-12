# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:read_tb_dwd_fin_ad_spend_df.py
@Time:2022/12/7 9:58
@Read: 广告上线时间及花费表; 可以确定广告上线的时间区间；以及该区间内的广告花费信息； 下面的所有结果均因为月度的top优化师不同而不同
"""
import gc
import random
import pandas as pd
from functions import *
from collections import Counter
import pickle

# todo 把所有好的优化师id筛选出来：所有的优化师的信息；按照月份统计；每个月份的这个结果可能会不同; 只保留每个家族top5的优化师作为top优化师
# FB平台：指定每个月后的Top5优化师ID信息；结果如下：如果没有新的时间节点；这个不需要再读；
# from 优化师_高度汇总表_tb_dws_ord_order_si_crt_df.EachHomeTop10ByMonth import all_top_apt

# alltopoptids = []
# apt_groups = all_top_apt.groupby('family_name').groups
# for k, v in apt_groups.items():
#     # fixme 下面的几个家族订单量太少；可能是和其自身的团队/线路/平台等相关；暂时不考虑
#     if k == '研发部' or k == '金蝉家族' or k == '金鹏家族' or k == '金牛家族' or k == '红杉家族' or k == '项目部' or k == '冰原狼家族':
#         continue
#     else:
#         alltopoptids += all_top_apt.loc[v]['opt_id'].to_list()

# todo 找到 most top的优化师：即每个月都会出现在top名单中的6个家族的优化师; 然后从这些opts中挑选最优的以革除偶然性；形成most top；
m22_11 = (
    32607, 631791, 32581, 632137, 631802, 81, 631525, 10344, 631299, 32025, 1719, 1787, 32260, 631647, 631116, 10230,
    1763,
    56, 10145, 632124, 631120, 1243, 32398, 32396, 631009, 631855, 1152, 32920, 631635, 10249)
m22_10 = (
    631802, 631791, 32607, 32581, 631917, 631525, 32470, 10344, 32025, 81, 1379, 1787, 1719, 32260, 2017, 10230, 56,
    1763,
    632124, 32867, 631009, 631120, 32398, 10223, 1475, 32920, 631855, 1617, 10249, 1141)
m22_9 = (
    631802, 631917, 631791, 32581, 951, 32470, 631525, 32025, 32195, 10344, 631647, 1719, 32260, 1787, 32603, 10230,
    1763,
    56, 33017, 632124, 4057, 631009, 32396, 32708, 32398, 10249, 4097, 631855, 631792, 1617)
m22_8 = (
    631802, 951, 32581, 32720, 32219, 32470, 32025, 631525, 631157, 32375, 1719, 1787, 32603, 32260, 631647, 10230,
    10078,
    1763, 56, 32176, 1713, 1475, 4057, 32708, 631863, 4097, 631229, 32243, 10249, 32920)
m22_7 = (
    32720, 32219, 32822, 32967, 32581, 32470, 32195, 32025, 32375, 32904, 1719, 32260, 1787, 32603, 2017, 10230, 10078,
    32176, 1763, 32144, 4057, 1713, 631863, 631120, 1475, 4097, 32243, 10249, 32920, 1617)
m22_6 = (
    32720, 32822, 32237, 32581, 32219, 32470, 32025, 32375, 10344, 32195, 1719, 631985, 32260, 32257, 2017, 10078,
    10230,
    56, 1763, 32144, 4057, 1475, 32297, 32396, 631318, 4097, 32243, 10249, 1141, 1617)
m22_5 = (
    32237, 32720, 32581, 33011, 631558, 32470, 32195, 32025, 631031, 32780, 32260, 1719, 1787, 32603, 631124, 10230,
    10078,
    56, 32176, 1763, 32396, 1713, 4056, 32488, 4057, 4097, 1141, 1152, 32243, 1617)
m22_4 = (
    32237, 32822, 32720, 32967, 32581, 32470, 32195, 10344, 631031, 32780, 1719, 1379, 32257, 32260, 32603, 10078, 56,
    10230, 1763, 32144, 32396, 4056, 4057, 1707, 32540, 4097, 32243, 1141, 1152, 1617)
m22_3 = (
    32237, 32967, 32822, 631730, 631171, 32470, 10320, 32780, 4257, 81, 32260, 32603, 2017, 32257, 631124, 10078, 10230,
    56,
    1763, 631448, 1243, 32396, 4056, 4057, 1707, 1617, 4097, 32243, 1152, 631635)
m21_12 = (
    32237, 631153, 32822, 32720, 32710, 10320, 4257, 32375, 32403, 32025, 32260, 32603, 32255, 1379, 1787, 56, 10230,
    10078,
    32352, 32144, 4057, 1713, 10223, 631045, 1707, 1617, 1152, 10249, 4097, 1141)
m21_11 = (
    922, 10063, 32822, 32807, 32714, 32470, 32025, 10320, 32289, 4257, 32603, 32260, 631116, 32073, 1719, 10146, 32352,
    32144, 10230, 56, 1475, 10223, 4057, 1506, 10260, 1617, 10249, 1152, 4097, 32243)
m21_10 = (
    33011, 32822, 10063, 32607, 32356, 32289, 32025, 32470, 32780, 110, 32603, 859, 1719, 1787, 32260, 32390, 32873,
    33017,
    10165, 32352, 1475, 32540, 1506, 1713, 10260, 32243, 1152, 4097, 1617, 32848)
m21_9 = (
    42, 32356, 32822, 32607, 32293, 32025, 217, 32780, 32289, 32909, 1787, 1379, 32603, 1719, 32260, 32390, 32450,
    32352,
    32353, 631200, 541, 4057, 1224, 1506, 32054, 10249, 1617, 1152, 32848, 32243)
m21_8 = (
    42, 758, 32710, 32822, 32607, 32780, 32025, 32375, 32195, 4240, 1379, 1787, 32603, 32251, 32260, 32353, 32390,
    32352,
    32450, 32873, 4057, 541, 1506, 1475, 1224, 1152, 10249, 1617, 32243, 32920)
m21_7 = (
    758, 32710, 922, 32822, 32581, 32025, 32375, 32780, 32195, 32289, 1379, 1787, 32251, 32256, 32603, 32353, 10165,
    10230,
    32144, 10145, 1506, 1475, 1224, 1713, 1310, 1152, 10249, 1617, 32243, 32848)
m21_6 = (
    32237, 42, 32581, 32607, 32580, 32718, 109, 32375, 425, 32327, 1379, 1787, 1719, 32603, 32260, 32353, 10165, 1763,
    32144, 10145, 1506, 541, 1475, 4057, 1224, 1152, 1617, 32879, 32243, 10249)
m21_5 = (
    32237, 32607, 42, 32293, 922, 32375, 108, 32195, 425, 461, 1719, 859, 1379, 1787, 32043, 32353, 10230, 32144, 10146,
    32680, 541, 535, 4057, 1506, 1475, 1617, 1152, 32243, 10249, 10128)
m21_4 = (
    32237, 32293, 922, 32174, 32607, 32375, 32403, 32452, 104, 10344, 1719, 1379, 859, 1787, 32318, 10230, 10145, 32144,
    10146, 32353, 4057, 1224, 541, 1243, 1506, 1617, 10249, 1152, 32243, 10141)
m21_3 = (
    32237, 32967, 32822, 631730, 631171, 32470, 10320, 32780, 4257, 81, 32260, 32603, 2017, 32257, 631124, 10078, 10230,
    56,
    1763, 631448, 1243, 32396, 4056, 4057, 1707, 1617, 4097, 32243, 1152, 631635)
# todo 从上数所有的相对top优化师中挑选出来most top优化师集合
allres = list(
    m22_3 + m22_4 + m22_5 + m22_6 + m22_7 + m22_8 + m22_9 + m22_10 + m22_11 + m21_3 + m21_4 + m21_5 + m21_6 + m21_7 + m21_8 + m21_9 + m21_10 + m21_11 + m21_12)
count_topopts = Counter(allres)
most_top = []
for k, v in count_topopts.items():
    if v >= 5:
        most_top.append(k)
# todo 然后再把每月的相对top优化师和上面的most top优化师作比较；就可以得到每月每个家族 most top 优化师投放的数据
eachmonth_most_top = []
tmp_eachmonth_most_top = [m21_12, m21_11, m21_10, m21_9, m21_8, m21_7, m21_6, m21_5, m21_4, m21_3,
                          m22_11, m22_10, m22_9, m22_8, m22_7, m22_6, m22_5, m22_4, m22_3]
for i in tmp_eachmonth_most_top:
    t = []
    for _ in i:
        if _ in most_top:
            t.append(_)
    eachmonth_most_top.append(t)

#  所有家族指定月份中most top优化师和other投放的广告数据；字段说明
""" CREATE TABLE IF NOT EXISTS tb_dwd_fin_ad_spend_df
(
    id                 STRING        COMMENT '自增主键',
    origin_id          STRING        COMMENT '原始id',
    spend_date         STRING        COMMENT '花费时间',
    befrom             STRING        COMMENT '花费平台',
    opt_id             BIGINT        COMMENT '优化师id',
    origin_spend       DECIMAL(24,6) COMMENT '花费金额(原币)',
    true_amount        DECIMAL(24,6) COMMENT '真实花费数据（人民币，汇率换算后数据，不包含分摊费用）',
    status_type        STRING        COMMENT '花费状态',
    sale_id            BIGINT        COMMENT '商品id',
    channel            BIGINT        COMMENT '渠道',
    do_user_id         BIGINT        COMMENT '操作用户id',  -- 该字段什么意思
    site_id            BIGINT        COMMENT '站点id',
    crt_time           STRING        COMMENT '操作时间',  -- 操作时间内操作了哪些内容
    tag                BIGINT        COMMENT '1商品，2分类，3活动，4站点',
    tag_id             BIGINT        COMMENT '标签id',
    add_cart           BIGINT        COMMENT '加购',
    add_pay_info       STRING        COMMENT '添加支付信息',
    checkout           BIGINT        COMMENT '结算',
    clicks             BIGINT        COMMENT '点击',
    conversions        BIGINT        COMMENT '转换',
    impressions        BIGINT        COMMENT '展示',
    purchase           BIGINT        COMMENT '购买',
    reach              BIGINT        COMMENT '触及人次',
    campaign_id        STRING        COMMENT '广告系列id',
    compaign_name      STRING        COMMENT '广告系列名称',
    team_code          STRING        COMMENT '团队id',
    org_code           STRING        COMMENT '组织编码',
    currency_id        STRING        COMMENT '货币id ',
    currency_name      STRING        COMMENT '货币名称',
    is_multi_currency  BIGINT        COMMENT '是否多币种',
    sale_name          STRING        COMMENT '商品名称',
    chooser_id         BIGINT        COMMENT '选品师id',
    designer_id        BIGINT        COMMENT '设计师id',
    product_id         BIGINT        COMMENT '产品id ',
    line_code          STRING        COMMENT '线路编码',
    line_name          STRING        COMMENT '线路名称',
    team_name          STRING        COMMENT '团队名称',
    family_name        STRING        COMMENT '家族名称',
    product_name       STRING        COMMENT '产品名称',
    category_id        BIGINT        COMMENT '产品分类ID',
    category_lvl1_name STRING        COMMENT '一级分类名称',
    category_lvl2_name STRING        COMMENT '二级分类名称',
    category_lvl3_name STRING        COMMENT '三级分类名称',
    ad_spend           STRING        COMMENT '广告花费（人民币，最终处理数据，包含分摊费用）',
    ad_account_id      STRING        COMMENT '广告账户ID',
    ad_account_name    STRING        COMMENT '广告账户名称',
    landing_page_view  STRING        COMMENT '链接点击量'
) 
COMMENT '广告花费表，按花费时间保存数据' PARTITIONED BY (pt   STRING    COMMENT '日分区');
"""

# todo 指定月份most top优化师和其他优化师的广告内容以及花费比较
target_month_head = '20211201'
target_month_end = '20221231'
sqltop = """
select
spend_date,product_id,category_lvl1_name as "一级",category_lvl2_name as "二级",category_lvl3_name as "三级",product_name, sale_id, sale_name, 
opt_id as "优化师id", chooser_id as "选品师id", designer_id as "设计师id", site_id as "站点ID", family_name,line_name,
-- do_user_id as "操作用户id", crt_time as "操作时间",

-- impressions 展示【展示量】和 reach 触及人次【多少人看到】的关系以及和 clicks 点击之间的关系；购买和结算有什么区别；
impressions as "展示", reach as "触及人次", clicks as "点击", add_cart as "加购", add_pay_info as "添加支付信息", checkout as "结算",
-- true_amount as '广告真实花费'和 ad_spend as '广告花费（人民币，最终处理数据，包含分摊费用）' 之间的区别；   true_amount as "广告真实花费",
campaign_id as "广告系列id", compaign_name as "广告系列名称", ad_spend as "广告花费（人民币，最终处理数据，包含分摊费用）"
from giikin_aliyun.tb_dwd_fin_ad_spend_df where befrom='facebook' 
and  pt>={} and  pt<={} -- important  1.这里优化师投放的广告数据
and opt_id in   -- important  2.要和月度的 most top 优化师相对应
-- 2022  N月份所有家族 most top 优化师ID
-- 11 
-- 10
-- 9
-- 8
-- 7
-- 6
-- 5
-- 4
-- 3
-- 2021 ----
-- 12
(32237, 32822, 32720, 32375, 32025, 32260, 32603, 1379, 1787, 56, 10230, 10078, 32352, 32144, 4057, 1713, 1617, 1152, 10249, 4097, 1141)
-- 11
-- 10
-- 9
-- 8
-- 7
-- 6
-- 5
-- 4
-- 3
""".format(target_month_head, target_month_end)

sqlotheropt = """
select
spend_date,product_id,category_lvl1_name as "一级",category_lvl2_name as "二级",category_lvl3_name as "三级",product_name, sale_id, sale_name, 
opt_id as "优化师id", chooser_id as "选品师id", designer_id as "设计师id", site_id as "站点ID", family_name,line_name,
-- do_user_id as "操作用户id", crt_time as "操作时间",

-- impressions 展示【展示量】和 reach 触及人次【多少人看到】的关系以及和 clicks 点击之间的关系；购买和结算有什么区别；
impressions as "展示", reach as "触及人次", clicks as "点击", add_cart as "加购", add_pay_info as "添加支付信息", checkout as "结算",
-- true_amount as '广告真实花费'和 ad_spend as '广告花费（人民币，最终处理数据，包含分摊费用）' 之间的区别；   true_amount as "广告真实花费",
campaign_id as "广告系列id", compaign_name as "广告系列名称", ad_spend as "广告花费（人民币，最终处理数据，包含分摊费用）"
from giikin_aliyun.tb_dwd_fin_ad_spend_df where befrom='facebook' 
and  pt>={} and  pt<={} -- important 这里优化师投放的广告数据要和月度的优化师相对应
and opt_id not in
-- 2022
-- 11
-- 10
-- 9
-- 8
-- 7
-- 6
-- 5
-- 4
-- 3
-- 2021  N月份所有家族 most top 优化师ID
-- 12 
(32237, 32822, 32720, 32375, 32025, 32260, 32603, 1379, 1787, 56, 10230, 10078, 32352, 32144, 4057, 1713, 1617, 1152, 10249, 4097, 1141)
-- 11
-- 10
-- 9
-- 8
-- 7
-- 6
-- 5
-- 4
-- 3
""".format(target_month_head, target_month_end)

# todo 优化师的广告花费表信息
with o.execute_sql(sqltop).open_reader(tunnel=True) as reader:
    ad_day2day_spend_top = reader.to_pandas()
    ad_day2day_spend_top = ad_day2day_spend_top.dropna(axis=0, subset=['product_name'])
with o.execute_sql(sqlotheropt).open_reader(tunnel=True) as reader:
    ad_day2day_spend_other = reader.to_pandas()
    ad_day2day_spend_other = ad_day2day_spend_other.dropna(axis=0, subset=['product_name'])

# some test 在给定一个广告id的情况下；确定这个广告的投放时间是多久; 且每个广告每天的花费是多少的计算；
# randomadid_tar = random.choice(ad_day2day_spend_top['广告系列id'].to_list())  # 随机挑选一个广告系列
# randomadid_tar_index = ad_day2day_spend_top[ad_day2day_spend_top.广告系列id == randomadid_tar].index.tolist()
# randomadid_tar_ad_content = ad_day2day_spend_top.loc[randomadid_tar_index]

# todo 获取每个月份中Most top和other优化师的数据；方便后续数据分析
# alltopopts_ads = {}
alltopopts_ads = []
optid_groups = ad_day2day_spend_top.groupby('优化师id').groups
for key, lineIndex in optid_groups.items():
    # alltopopts_ads[key] = ad_day2day_spend_top.loc[lineIndex]
    alltopopts_ads.append(ad_day2day_spend_top.loc[lineIndex])
alltopopts_ads_df = pd.concat(alltopopts_ads)

del alltopopts_ads
del ad_day2day_spend_top
gc.collect()

# allotheropts_ads = {}
allotheropts_ads = []
optid_groups = ad_day2day_spend_other.groupby('优化师id').groups
for key, lineIndex in optid_groups.items():
    # allotheropts_ads[key] = ad_day2day_spend_other.loc[lineIndex]
    allotheropts_ads.append(ad_day2day_spend_other.loc[lineIndex])
allotheropts_ads_df = pd.concat(allotheropts_ads)

del allotheropts_ads
del ad_day2day_spend_other
gc.collect()

# todo Direc - 按照线路和家族区分优化师：观察每个月份的mostTop优化师和其它优化师之间的产品投放差异 + 同时也有优化师月度的投放数量（速度）
# wait：在得到most top优化师最喜欢投放的线路之后：线路上的人群/线路上的商品特征等等；
line_name_family_name_other = allotheropts_ads_df.groupby(['line_name', 'family_name'])
line_name_family_name_other = [{i[0]: i[1]} if i[1].shape[0] > 50 else 0 for i in
                               line_name_family_name_other]  # > 50 说明是线路上无异常的商品
while 0 in line_name_family_name_other:
    line_name_family_name_other.remove(0)

line_name_family_name_top = alltopopts_ads_df.groupby(['line_name', 'family_name'])
line_name_family_name_top = [{i[0]: i[1]} if i[1].shape[0] > 50 else 0 for i in line_name_family_name_top]
while 0 in line_name_family_name_top:
    line_name_family_name_top.remove(0)

# todo 在得到优化师投放的广告花费的详细数据后；就是产品和广告组/广告系列之间的关系；因此读取下面的数据；
with open('../../../优化师_投放的广告涉及的产品信息_gdsc/gk_product_report_fb.pick', 'rb') as f:
    gk_product_report_fb = pickle.load(f)
with open('../../优化师_产品涉及的广告系列以及系列日报/giikinadcampaign.pick', 'rb') as f:
    giikin_campaign = pickle.load(f)
with open('../../优化师_产品涉及的广告系列以及系列日报/giikinadcampaign_report.pick', 'rb') as f:
    giikin_campaign_report = pickle.load(f).drop(['经营分析-花费RMB', '经营评估结果，HIGHT, MID, LOW, LOW2'], axis=1)  # 删除两个多余列
with open('../优化师_广告系列下的广告组数据/adgroup.pick', 'rb') as f:
    giikinad_groups = pickle.load(f)
    giikinad_groups.rename(columns={'广告系列ID': 'campaign_id'}, inplace=True)

# 把广告组和广告系列关联起来；进而按照系列和优化师进行划分
context_adcam_ad_groups = pd.merge(giikin_campaign, giikinad_groups, on='campaign_id', how='left'). \
    dropna(axis=0, subset='category')  # axis=0 按行删除数据
# context_adcam_ad_groups = giikin_campaign.set_index('campaign_id').join(giikinad_groups.set_index('广告系列ID'))
context_adcam_ad_groups.drop(
    ['id', 'ID：平台＋账户ID＋系列ID＋广告组ID', 'start_time', '源广告组ID：当前广告组是从哪个广告组复制来的', '日预算', '生命周期预算'],
    axis=1)
del giikin_campaign
del giikinad_groups
gc.collect()

breaks = 'break'

# 把广告广告系列和广告组和优化师关联起来
# context_adcam_ad_groups.groupby('')

# lines_other = allotheropts_ads_df.groupby('line_name').groups  # groups
# lines_other = [{k: allotheropts_ads_df.loc[v]} for k, v in lines_other.items()]
# lines_top = alltopopts_ads_df.groupby('line_name').groups  # groups
# lines_top = [{k: alltopopts_ads_df.loc[v]} for k, v in lines_top.items()]
# each_line_goods_other = {}
# for _ in lines_other:
#     for line_name, line_df in _.items():
#         if line_df.shape[0] <= 50:
#             continue
#         each_line_goods_other[line_name] = line_df
# each_line_goods_top = {}
# for _ in lines_top:
#     for line_name, line_df in _.items():
#         if line_df.shape[0] <= 50:
#             continue
#         each_line_goods_top[line_name] = line_df

# 投放的品类差异
# count_cat_top = {}
# count_cat_other = {}
# for k, v in each_line_goods_top:
#     v.
# test
# randomadid_tar = random.choice(each_line_goods_other['英国']['广告系列id'].to_list())
# randomadid_tar_index = each_line_goods_other['英国'][each_line_goods_other['英国'].广告系列id == randomadid_tar].index.tolist()
# randomadid_tar_ad_content = each_line_goods_other['英国'].loc[randomadid_tar_index]

# todo 每个月份下：优化师投放的广告时间的分布：好坏优化师、线路上的广告存活时间；
# for p in lines:
#     if each_line_goods_other[p].shape[0] <= 50:  # 这种情况下不是正常的由优化师投放出去的广告
#         continue
#     each_line_goods_other[p][]


# camid_groups = ad_day2day_spend_top.groupby('广告系列id').groups

# wait 广告组在广告增长和衰退期间的广告组的扩充与减少的情况 + 广告投放期间订单的情况

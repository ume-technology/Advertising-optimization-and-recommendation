# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:读取所有商品对应的广告系列数据.py
@Time:2022/11/29 16:30
@Read: 这里是把广告系列和广告系列的report数据加载进来
"""
import pickle

# todo 核心数据，可以用来帮助获取每个优化投放的广告，包括TOP优化师的商品数据
# todo giikinadcampaign 和 giikinadcampaign_report 广告系列数据和广告系列经营分析数据的合并 - 广告系列是全的，包含了所有优化师投放的所有广告
# 读取的是商品系列数据信息
with open('giikinadcampaign.pick', 'rb') as f:
    giikin_campaign = pickle.load(f)
# campaign_id_groups = giikin_campaign.groupby('campaign_id').groups
# campaign_id_groups = giikin_campaign.groupby('campaign_id')  # one campaignID one goods

# 读取的是基于商品系列的统计信息
with open('giikinadcampaign_report.pick', 'rb') as f:
    giikin_campaign_report = pickle.load(f)
# campaign_id__report_groups = giikin_campaign_report.groupby('gk_campaign_id').groups
# campaign_id__report_groups = giikin_campaign_report.groupby('gk_campaign_id')
# todo 给字段重命名
#
# todo 删除一些字段


# todo 对广告campaign的report按照id做汇总
giikin_campaign_report_groups = giikin_campaign_report.groupby('gk_campaign_id').groups
for key, lineindex in giikin_campaign_report_groups.items():
    if key == 2:
        res = giikin_campaign_report.loc[lineindex]
        res['date'] = res.to_datetime(res['date'])

# 优化师投放出来的广告和广告的report数据信息的关联
context = giikin_campaign.set_index('id').join(giikin_campaign_report.set_index('gk_campaign_id'))
context_ader_id_groups = context.groupby('ader_id').groups
context_ader_id_groups_keys_list = context_ader_id_groups.keys()

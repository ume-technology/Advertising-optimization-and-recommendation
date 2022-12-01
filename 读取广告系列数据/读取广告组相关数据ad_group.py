# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author: UmeAI
@Blog(个人博客地址): https://www.umeai.top
@File:读取广告组相关数据ad_group.py
@Time:2022/11/30 17:10
@todo: 读取广告组的相关表
@fixme: 
@important: 
"""

import pickle
import pymysql
import pandas as pd


def readdatafromdatabase(sql):
    host = 'dtsproxy.giikina.xyz'
    port = 33316
    db = 'adapi-online'
    user = 'fanzhimin'
    passwd = 'V^@2UHjkTxcNxMj#'
    conn = pymysql.connect(host=host, port=port, user=user, db=db, password=passwd, charset='utf8')
    dataDF = pd.read_sql(sql, conn)
    return dataDF


if __name__ == '__main__':
    sql_adgroup = """
    select
    id as 'ID：平台＋账户ID＋系列ID＋广告组ID' , 
    campaign_id as '广告系列ID',m_source_adset_id as '源广告组ID：当前广告组是从哪个广告组复制来的',ad_group_id as '广告组ID', 
    ad_group_name as '广告组名称', m_start_time as '开始时间',daily_budget_micro as '日预算',lifetime_budget_micro as '生命周期预算'
     from ad_group where platform = 'facebook' """
    dataDF = readdatafromdatabase(sql_adgroup)
    with open('adgroup.pick', 'wb') as f:
        pickle.dump(dataDF, f)

"""
CREATE TABLE `ad_group` (
  `id` varchar(150) NOT NULL COMMENT 'ID：平台＋账户ID＋系列ID＋广告组ID',
  `create_by` varchar(50) DEFAULT NULL COMMENT '创建人',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `modify_by` varchar(50) DEFAULT NULL COMMENT '修改人',
  `update_time` datetime DEFAULT NULL COMMENT '修改时间',
  `ad_account_id` varchar(50) DEFAULT NULL COMMENT '广告账户ID',
  `ad_group_id` varchar(50) DEFAULT NULL COMMENT '广告组ID',
  `ad_group_name` text CHARACTER SET utf8mb4 COMMENT '广告组名称',
  `campaign_id` varchar(50) DEFAULT NULL COMMENT '广告系列ID',
  `m_configured_status` varchar(20) DEFAULT NULL COMMENT '广告组投放状态',
  `m_effective_status` varchar(20) DEFAULT NULL COMMENT '广告组投放状态：包含上级（如广告系列）的投放状态',
  `m_end_time` datetime DEFAULT NULL COMMENT '结束时间',
  `m_source_adset_id` varchar(50) DEFAULT NULL COMMENT '源广告组ID：当前广告组是从哪个广告组复制来的',
  `m_start_time` datetime DEFAULT NULL COMMENT '开始时间',
  `m_status` varchar(20) DEFAULT NULL COMMENT '状态',
  `m_updated_time` datetime DEFAULT NULL COMMENT '更新时间',
  `platform` varchar(20) DEFAULT NULL COMMENT '平台',
  `targeting` longtext COMMENT '受众',
  `bid_strategy` varchar(50) DEFAULT NULL COMMENT '竞价策略',
  `daily_budget_micro` bigint(20) NOT NULL COMMENT '日预算',
  `lifetime_budget_micro` bigint(20) NOT NULL COMMENT '生命周期预算', 
  `batchverno` varchar(50) DEFAULT NULL COMMENT '已废弃',
  `bid_amount` bigint(20) DEFAULT NULL,
  `billing_event` varchar(50) DEFAULT NULL,
  `issues_info` text CHARACTER SET utf8mb4,
  `m_created_time` datetime DEFAULT NULL,
  `optimization_goal` varchar(50) DEFAULT NULL,
  `pacing_type` varchar(50) DEFAULT NULL,
  `pixel_id` varchar(50) DEFAULT NULL,
  `promoted_object` text CHARACTER SET utf8mb4,
  PRIMARY KEY (`id`),
  KEY `IDXkafm96du8fftgmoeoxk751ec3` (`platform`,`ad_account_id`,`campaign_id`,`ad_group_id`),
  KEY `IDXk7b9sqykacwnthhlood63l1n5` (`update_time`),
  KEY `IDX5xpnks9d7wkromf7x7m2dwiw9` (`batchverno`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='广告组实体表';
"""
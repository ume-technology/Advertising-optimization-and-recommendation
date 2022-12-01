# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:read_apionline_gk_campaign.py
@Time:2022/11/26 15:18
@Read: 读取广告系列相关数据；读取的是优化从产品出发，投放出来的广告系列数据； 只选择了投放在FB平台的广告系列;
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
    sql_campaign = """select 
    id,account_id, campaign_id,campaign_name, 
    ader_id,chooser_id,chooser_name,designer_id,designer_name,
    goods_id as '产品ID', goods_name as '产品名', category,product_id as '商品ID', product_name as '商品名',
    start_time
    from gk_campaign where platform = 'facebook' """
    dataDF = readdatafromdatabase(sql_campaign)
    with open('giikinadcampaign.pick', 'wb') as f:
        pickle.dump(dataDF, f)

sql = """
CREATE TABLE `gk_campaign` (
  `is_change` bit(1) DEFAULT b'0' COMMENT '是否改派 0不是,1 是',
  `is_test_product` bit(1) DEFAULT b'0' COMMENT '/是否测品 ，0 不是， 1 是',
  `low_price` bit(1) DEFAULT b'0' COMMENT '是否低价  0不是 ，1 是',
  `org_code` varchar(50) DEFAULT NULL COMMENT '部门编号',
  `platform2` varchar(50) DEFAULT NULL COMMENT '经营分析平台',
  `product_id` int(11) DEFAULT '0' COMMENT '商品id',
  `product_name` varchar(255) DEFAULT NULL COMMENT '商品名称',
  `product_type` varchar(5) DEFAULT NULL COMMENT '商品类型，A B C',
  `lifetime_budget` decimal(19,2) DEFAULT NULL COMMENT '总预算 或 生命周期预算',
  `platform` varchar(50) DEFAULT NULL COMMENT '广告平台',
  `start_time` datetime DEFAULT NULL COMMENT '开始时间',
  `status` varchar(50) DEFAULT NULL COMMENT '系列状态，设置的状态',
  `active` bit(1) DEFAULT NULL COMMENT '是否有效',
  `last_active` datetime DEFAULT NULL COMMENT '最近一次改动',
  `last_create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '记录-创建时间',
  `last_local_event` datetime DEFAULT NULL COMMENT '最近一次本地事件改动',
  `last_local_event_check` datetime DEFAULT NULL,
  `last_order` datetime DEFAULT NULL COMMENT '最近一次订单改动',
  `last_order_check` datetime DEFAULT NULL,
  `last_sale_rel` datetime DEFAULT NULL COMMENT '最近一次广告关联改动',
  `last_sale_rel_check` datetime DEFAULT NULL,
  `last_spend` datetime DEFAULT NULL COMMENT '最近一次花费改动',
  `last_spend_check` datetime DEFAULT NULL,
  `last_update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录-更新时间',
  `target_cpa` decimal(19,2) DEFAULT NULL COMMENT '目标出价金额',
  `type` varchar(50) DEFAULT NULL COMMENT '系列类型/系列目标',


  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '系列id索引',
  `account_id` varchar(50) DEFAULT NULL COMMENT '广告账号id',
  `bid_amount` decimal(19,2) DEFAULT NULL COMMENT '竞价金额',  # 都是 None 就不选择了,
  `bid_strategy` varchar(50) DEFAULT NULL COMMENT '竞价策略',  # 也都是 None 就不选择了,
  `budget_type` varchar(50) DEFAULT NULL COMMENT '预算类型',  # 都是 none 就不选择了,
  `campaign_id` varchar(50) DEFAULT NULL COMMENT '系列id',
  `campaign_name` varchar(500) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '系列名称',
  `daily_budget` decimal(19,2) DEFAULT NULL COMMENT '日预算',   # none,
  `end_time` datetime DEFAULT NULL COMMENT '结束时间', 
  `ader_id` int(11) DEFAULT '0' COMMENT '优化师id',
  `ader_name` varchar(50) DEFAULT NULL COMMENT '优化师名称',
  `area_id` int(11) DEFAULT '0' COMMENT '团队id',
  `area_name` varchar(50) DEFAULT NULL COMMENT '团队名称',
  `category` varchar(255) DEFAULT NULL COMMENT '产品分类名称',
  `category_id` int(11) DEFAULT '0' COMMENT '产品分类id',
  `chooser_id` int(11) DEFAULT '0' COMMENT '选品id',
  `chooser_name` varchar(50) DEFAULT NULL COMMENT '选品名称',
  `coll_id` int(11) DEFAULT '0' COMMENT '站点id',
  `currency_id` int(11) DEFAULT '0' COMMENT '老币种id',
  `designer_id` int(11) DEFAULT '0' COMMENT '设计id',
  `designer_name` varchar(50) DEFAULT NULL COMMENT '设计名称',
  `domain` varchar(250) DEFAULT NULL COMMENT '域名',
  `goods_id` int(11) DEFAULT '0' COMMENT '产品id',
  `goods_name` varchar(255) DEFAULT NULL COMMENT '产品名称',

  PRIMARY KEY (`id`),
  UNIQUE KEY `UK1bjaf7nkamvo4a39qt8xxaket` (`campaign_id`,`platform`),
  KEY `IDXnswbc9m1lod02nbpivf3c3267` (`account_id`,`platform`)
) ENGINE=InnoDB AUTO_INCREMENT=564618 DEFAULT CHARSET=utf8;
<<<<<<< HEAD
=======


>>>>>>> 57b879aa2dbeaa06d57e512f09ddd34c008f73e4
"""

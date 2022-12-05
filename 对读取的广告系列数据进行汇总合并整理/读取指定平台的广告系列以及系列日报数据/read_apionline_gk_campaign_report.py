    # !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:read_apionline_gk_campaign_report.py
@Time:2022/11/26 15:19
@Read: 优化师投放出来的广告的经营分析情况
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
    # fixme
    #      ad_cost as '经营分析-花费RMB',
    #      ad_spend_rmb as  '广告-原始花费', 两者区别是什么
    sql_campaign_report = """ select 
    gk_campaign_id,
    ad_impression as '广告-展示量',ad_clicks as '广告-点击量',
     e_checkout as '本地事件-添加支付信息量',e_purchase as '本地事件-购买量，或 转化量',
     o_cod_count as '到付有效订单量',cod_order_count as '经营分析-到付订单量',
     o_online_count as '在线有效订单量',online_order_count as  '在线支付订单量',
     high_line as '利润率高点，', low_line as '利润率低点，', mid_line as '利润率中间点，',
     profit_rate_result as '经营评估结果，HIGHT, MID, LOW, LOW2',
     ad_cost as '经营分析-花费RMB',
     ad_spend_rmb as  '广告-原始花费',
     report_date as '报告日期'
     from  gk_campaign_report"""  # 按照广告系列进行的商品统计
    dataDF = readdatafromdatabase(sql_campaign_report)
    with open('giikinadcampaign_report.pick', 'wb') as f:
        pickle.dump(dataDF, f)

sql = """
CREATE TABLE `gk_campaign_report` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '系列日报告自增id',
  `ad_checkout` int(11) unsigned DEFAULT '0' COMMENT '广告-添加支付信息量',
  `e_checkout` int(11) unsigned DEFAULT '0' COMMENT '本地事件-添加支付信息量',
  `e_purchase` int(11) unsigned DEFAULT '0' COMMENT '本地事件-购买量，或 转化量',
  `ad_purchase` int(11) unsigned DEFAULT '0' COMMENT '广告-购买量，或转化量',
  `o_cod_count` int(11) unsigned DEFAULT '0' COMMENT '订单-到付有效订单量',
  `o_online_count` int(11) unsigned DEFAULT '0' COMMENT '订单-在线有效订单量',
  `ad_impression` int(11) unsigned DEFAULT '0' COMMENT '广告-展示量',
  `ad_clicks` int(11) unsigned DEFAULT '0' COMMENT '广告-点击量',
  `high_line` decimal(19,2) DEFAULT '0.00' COMMENT '经营分析- 利润率高点，',
  `low_line` decimal(19,2) DEFAULT '0.00' COMMENT '经营分析- 利润率低点，',
  `mid_line` decimal(19,2) DEFAULT '0.00' COMMENT '经营分析- 利润率中间点，',
  `profit_rate_result` varchar(50) DEFAULT NULL COMMENT '经营分析- 经营评估结果，HIGHT, MID, LOW, LOW2',

  `e_update_time` datetime DEFAULT NULL COMMENT '本地事件-更新事件',
  `ad_cost` decimal(19,2) unsigned DEFAULT '0.00' COMMENT '经营分析-花费RMB',
  `ad_spend_rmb` decimal(19,4) unsigned DEFAULT '0.0000' COMMENT '广告-原始花费',
  `ad_update_time` datetime DEFAULT NULL COMMENT '广告-报告更新事件',
  `buy_price` decimal(19,2) unsigned DEFAULT '0.00' COMMENT '经营分析-采购成本RMB',
  `cod_logistics_fee` decimal(19,2) unsigned DEFAULT '0.00' COMMENT '经营分析- 到付运费 RMB',
  `cod_order_count` int(11) unsigned DEFAULT '0' COMMENT '经营分析-到付订单量',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '经营分析-创建时间',

  `first_send_qty` int(11) unsigned DEFAULT '0' COMMENT '经营分析-商品数量',
  `gift_cost` decimal(19,2) unsigned DEFAULT '0.00' COMMENT '经营分析-赠品话费RMB',

  `need_update` bit(1) DEFAULT NULL COMMENT '是否需要更新经营分析',
  `need_update_time` datetime DEFAULT NULL,
  `o_amount_rmb` decimal(19,2) unsigned DEFAULT '0.00' COMMENT '订单-有效订单额，RMB',

  `o_update_time` datetime DEFAULT NULL COMMENT '订单-更新事件',
  `online_logistics_fee` decimal(19,2) unsigned DEFAULT '0.00' COMMENT '经营分析- 在线支付运费 RMB',
  `online_order_count` int(11) unsigned DEFAULT '0' COMMENT '经营分析-在线支付订单量',
  `order_amount` decimal(19,2) unsigned DEFAULT '0.00' COMMENT '经营分析-订单额RMB',
  `other_rate` decimal(19,2) unsigned DEFAULT '0.00' COMMENT '经营分析- 其他费用占比 0-100',

  `report_date` date DEFAULT NULL COMMENT '报告日期',
  `second_send_qty` int(11) unsigned DEFAULT '0' COMMENT '经营分析-二次改派商品数量',
  `sign_rate` decimal(19,2) unsigned DEFAULT '0.00' COMMENT '经营分析- 签收率 0-100',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '经营分析-更新时间',
  `gk_campaign_id` int(11) NOT NULL COMMENT '系列id索引',
  PRIMARY KEY (`id`),
  KEY `IDXpyv58wtyjv82mhc2ffwq51n22` (`gk_campaign_id`),
  KEY `IDXq9t4vyg396n7dcxhd358jgsuo` (`report_date`),
  KEY `IDX739oae5fggpqjut2v56f0645w` (`need_update_time`),
  CONSTRAINT `FK5ey9xtnje7b176ciygkq55fmw` FOREIGN KEY (`gk_campaign_id`) REFERENCES `gk_campaign` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1162853 DEFAULT CHARSET=utf8;
"""

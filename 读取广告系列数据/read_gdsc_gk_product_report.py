# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:read_gdsc_gk_product_report.py
@Time:2022/11/26 14:58
@Read: 优化师投放的广告涉及到的产品数据表；但是这里只在sql中读取了FB的数据信息
"""
import pickle
import pymysql
import pandas as pd


def readdatafromdatabase():
    host = 'ro.hwaurora.rdsdb.com'
    port = 3306
    db = 'gdsc'
    user = 'fanzhimin'
    passwd = 'x%HpQhq8m9c#5zx@'
    conn = pymysql.connect(host=host, port=port, user=user, db=db, password=passwd, charset='utf8')
    sql = """select 
    goods_id as '产品ID', product_id as '商品ID',category as '产品分类' ,product_type as '商品类型' ,is_test_product, ader_id,create_time,update_time,
    order_date,cod_order_count as '到付有效订单数合计',online_order_count as '在线有效订单数合计', 
    amount_per_customer as '客单价', count_per_customer as '客单量', order_amount as '总销售额合计', signed_amount as '签收金额' ,
    cod_sign_rate as '到付签收率',online_sign_rate as '在线支付签收率',pre_sign_rate,his_sign_rate,

    gift_cost, buy_price, ad_cost ,online_logistics_fee as '线付运费合计', cod_logistics_fee as '到付运费合计', 
    buy_rate as '采购费用占比', ad_rate as '广告费占比',logistics_rate as '运费占比', other_rate as '其他费用占比',three_rate as '三项成本占比',

    high_line as '率润率高点', low_line as '率润率低点',profit_rate as '利润率', profit_rate_result as '利润率结果', lowest_roi as 'roi临界点',
    coll_type as '单页/商城/聚合页/等',
    roi
    from gk_product_report where platform='facebook' and invalid=0;"""

    # wait 广告投放时间；
    # wait 订单层面
    #   1.二次改派订单和商品订单之间的关系是什么，主要是这两个字段：
    #        `first_send_qty` int(10) DEFAULT '0' COMMENT '商品总数量',
    #        `second_send_qty` int(10) DEFAULT '0' COMMENT '二次改派订单的商品数量',
    #   2.`cod_order_count` int(11) DEFAULT NULL COMMENT '到付有效订单数合计',
    #   3.`online_order_count` int(11) DEFAULT NULL COMMENT '在线有效订单数合计',
    # wait 销售额层面：
    #       `order_amount` decimal(10,2) DEFAULT NULL COMMENT '总销售额合计',
    #       `cod_order_amount` decimal(10,2) DEFAULT NULL COMMENT 'cod销售额合计',    null ？
    #       `online_order_amount` decimal(10,2) DEFAULT NULL COMMENT 'online销售额合计',    null ？
    # wait 成本层面：
    #       `other_rate` decimal(10,2) DEFAULT NULL COMMENT '其他费用占比',
    #       `gift_cost` decimal(10,2) DEFAULT NULL COMMENT '赠品采购总成本合计', gift_cost = 0 ？
    #       `buy_price` decimal(10,2) DEFAULT NULL COMMENT '采购费用合计',       buy_price = 0 ？
    #       `three_rate` decimal(10,2) DEFAULT NULL COMMENT '三项成本占比',    哪三项
    # wait 利润层面
    #       `high_line` decimal(10,2) DEFAULT NULL COMMENT '率润率高点',
    #       `low_line` decimal(10,2) DEFAULT NULL COMMENT '率润率低点',
    # wait coll_type 如何区分广告对应的商品位置

    # todo cursor
    # cursor = conn.cursor()
    # count_ = cursor.execute(sql)  # 返回的是查询到的数据库的数据条目数量
    # print(count_)
    # data = cursor.fetchall()

    # todo pandas
    dataDF = pd.read_sql(sql, conn)
    return dataDF


if __name__ == '__main__':
    dataDF = readdatafromdatabase()
    # with open('gk_product_report_fb.pick', 'wb') as f:
    #     pickle.dump(dataDF, f)

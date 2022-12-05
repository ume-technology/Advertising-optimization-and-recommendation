# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:每个月的每个家族前十名优化师s.py
@Time:2022/11/28 17:38
@Read: 在每个月基于所有家族的的Top优化师s.sql文件中，不能准确地找到目标的优化师s的投放数据；因此在这里采用这个脚本中的sql进行top优化师s相关信息的查询
"""
from odps import ODPS
from odps import options


ACCESS_ID = 'LTAI5tHkaaaa5iivK9FoBk'
ACCESS_KEY = 'TKrkyHQbVDaaaaaaULi85h0TJxYzo'
DEFAULT_PROJECT = 'cda'
END_POINT = 'http://service.cn-shenzhen.maxcompute.aliyun.com/api'

o = ODPS(ACCESS_ID, ACCESS_KEY, DEFAULT_PROJECT, endpoint=END_POINT)
options.tunnel.use_instance_tunnel = True
options.tunnel.limit_instance_tunnel = False  # 关闭limit限制，读取全部数据

sql = """
select * from (
select family_name, opt_name,orders,ROW_NUMBER() over(partition  by family_name order by orders desc) rank from (
select  family_name,opt_name, sum(effect_order_cnt) orders, opt_id
-- team_name, sum(effect_order_cnt),sum(freight+charge_fee),sum(order_cny_amt),sum(freight+charge_fee)/sum(order_cny_amt)
 from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221101' and  pt<='20221127'
 and befrom='facebook'
 and opt_name is not null
 group by  family_name,opt_name) a 
 group by family_name,opt_name,orders) a 
 where rank <= 10
"""
sql = """
select * from (
select family_name,opt_id,opt_name,orders,ROW_NUMBER() over(partition  by family_name order by orders desc) rank from (
select  family_name,opt_id,opt_name, befrom, sum(effect_order_cnt) orders
-- team_name, sum(effect_order_cnt),sum(freight+charge_fee),sum(order_cny_amt),sum(freight+charge_fee)/sum(order_cny_amt)
 from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221101' and   pt<='20221127' and  befrom='facebook'
 and opt_name is not null
 group by  family_name,opt_id,opt_name, befrom) a 
 group by family_name,opt_id,opt_name,orders) a 
 where rank<=10
"""
with o.execute_sql(sql).open_reader(tunnel=True) as reader:
    df = reader.to_pandas()

"""
高度汇总表
CREATE TABLE IF NOT EXISTS tb_dws_ord_order_si_crt_df
(
    crt_time                    STRING        COMMENT '订单创建时间',
    befrom                      STRING        COMMENT '平台',
    sale_id                     BIGINT        COMMENT '商品id',
    site_id                     BIGINT        COMMENT '站点id',
    line_code                   STRING        COMMENT '线路编码 ',
    line_name                   STRING        COMMENT '线路名称',
    opt_id                      BIGINT        COMMENT '优化师id',
    opt_name                    STRING        COMMENT '优化师姓名',
    opt_org_code                STRING        COMMENT '优化师组织编码',
    campaign_id                 STRING        COMMENT '广告系列id',
    team_code                   STRING        COMMENT '团队',
    team_name                   STRING        COMMENT '团队名称',
    family_name                 STRING        COMMENT '家族名称',
    company                     STRING        COMMENT '分公司名称',
    currency_id                 BIGINT        COMMENT '币种id',
    currency_name               STRING        COMMENT '币种名称',
    lang_id                     BIGINT        COMMENT '语种id',
    lang_name                   STRING        COMMENT '语种名称',
    org_code                    STRING        COMMENT '组织编码',
    org_name                    STRING        COMMENT '组织名称',
    product_id                  BIGINT        COMMENT '产品id',
    product_name                STRING        COMMENT '产品名称',
    category_id                 BIGINT        COMMENT '品类ID',
    category_lvl1_name          STRING        COMMENT '一级分类名称',
    category_lvl2_name          STRING        COMMENT '二级分类名称',
    category_lvl3_name          STRING        COMMENT '三级分类名称',
    buyer_id                    BIGINT        COMMENT '采购员id',
    buyer_name                  STRING        COMMENT '采购员姓名',
    buyer_org_code              STRING        COMMENT '采购员组织编码',
    designer_id                 BIGINT        COMMENT '设计师id',
    designer_name               STRING        COMMENT '设计师姓名',
    designer_org_code           STRING        COMMENT '设计师组织编码',
    chooser_id                  BIGINT        COMMENT '选品师id',
    chooser_name                STRING        COMMENT '选品师姓名',
    chooser_org_code            STRING        COMMENT '选品师组织编码',
    all_order_cnt               BIGINT        COMMENT '总订单数',
    effect_order_cnt            BIGINT        COMMENT '有效订单数',
    online_pay_order_cnt        BIGINT        COMMENT '在线支付订单量',
    direct_order_cnt            BIGINT        COMMENT '直发订单数',
    change_order_cnt            BIGINT        COMMENT '改派订单数',
    wait_send_order_cnt         BIGINT        COMMENT '待发货订单数',
    allshipped_order_cnt        BIGINT        COMMENT '已发货订单数',
    signed_order_cnt            BIGINT        COMMENT '已签收订单数（包含签收、理赔、自发头程丢件）',
    signed_direct_order_cnt     BIGINT        COMMENT '已签收直发订单数（包含签收、理赔、自发头程丢件）',
    rejected_order_cnt          BIGINT        COMMENT '已拒收订单数（包含拒收、客户取消、销毁）',
    rejected_direct_order_cnt   BIGINT        COMMENT '已拒收直发订单数（包含拒收、客户取消、销毁）',
    refund_order_cnt            BIGINT        COMMENT '已退款订单数',
    refund_direct_order_cnt     BIGINT        COMMENT '已退款直发订单数',
    finish_status_cnt           BIGINT        COMMENT ' 已收款数',
    unusual_weight_cnt          BIGINT        COMMENT ' 重量异常订单量',
    unusual_freight_cnt         BIGINT        COMMENT ' 物流费用异常订单量',
    signed_cny_amt              DECIMAL(24,6) COMMENT '已签收订单金额（人民币）（包含签收、理赔、自发头程丢件）',
    signed_direct_cny_amt       DECIMAL(24,6) COMMENT '已签收直发订单金额（人民币）（包含签收、理赔、自发头程丢件）',
    rejected_cny_amt            DECIMAL(24,6) COMMENT '已拒收订单金额（人民币）（包含拒收、客户取消、销毁）',
    rejected_direct_cny_amt     DECIMAL(24,6) COMMENT '已拒收直发订单金额（人民币）（包含拒收、客户取消、销毁）',
    refund_cny_amt              DECIMAL(24,6) COMMENT '已退款订单金额（人民币）',
    refund_direct_cny_amt       DECIMAL(24,6) COMMENT '已退款直发订单金额（人民币）',
    order_amt                   DECIMAL(24,6) COMMENT '销售额（原币）',
    order_cny_amt               DECIMAL(24,6) COMMENT '人民币销售额',
    direct_cny_amt              DECIMAL(24,6) COMMENT '直发订单有效订单金额（人民币）',
    order_price                 DECIMAL(24,6) COMMENT '采购成本',
    direct_order_price          DECIMAL(24,6) COMMENT '直发采购成本',
    third_platform_fee          DECIMAL(24,6) COMMENT '三方平台费用',
    direct_first_freight        DECIMAL(24,6) COMMENT '直发头程运费',
    second_journey_freight      DECIMAL(24,6) COMMENT '尾程运费',
    direct_second_freight       DECIMAL(24,6) COMMENT '直发尾程运费',
    charge_fee                  DECIMAL(24,6) COMMENT '手续费',
    direct_charge_fee           DECIMAL(24,6) COMMENT '直发手续费',
    freight                     DECIMAL(24,6) COMMENT '物流运费(全程,不含手续费)',
    direct_freight              DECIMAL(24,6) COMMENT '直发物流运费(全程,不含手续费)',
    reach_cnt                   BIGINT        COMMENT '触及人次数',
    impressions_cnt             BIGINT        COMMENT '展示数',
    clicks_cnt                  BIGINT        COMMENT '点击数',
    add_pay_cnt                 BIGINT        COMMENT '添加支付信息数',
    conversions_cnt             BIGINT        COMMENT '转换数',
    purchase_cnt                BIGINT        COMMENT '购买数 ',
    spend_amt                   DECIMAL(24,6) COMMENT '花费金额',
    sale_cnt                    BIGINT        COMMENT '商品数量',
    online_order_cnt            BIGINT        COMMENT '在途订单量',
    refund_change_order_cnt     BIGINT        COMMENT '已退款改派订单量',
    finish_order_cnt            BIGINT        COMMENT '已完成订单量',
    finish_direct_order_cnt     BIGINT        COMMENT '已完成直发订单量',
    finish_change_order_cnt     BIGINT        COMMENT '已完成改派订单量',
    effect_order_cnt_no_cn      BIGINT        COMMENT '有效订单量(不含国内订单)',
    signed_change_order_cnt     BIGINT        COMMENT '已签收改派订单数（包含签收、理赔、自发头程丢件）',
    rejected_change_order_cnt   BIGINT        COMMENT '已拒收改派订单数（包含拒收、客户取消、销毁）',
    cod_effect_order_cnt        BIGINT        COMMENT '到付有效订单量',
    change_cny_amt              DECIMAL(24,6) COMMENT '改派订单有效订单金额（人民币）',
    signed_change_cny_amt       DECIMAL(24,6) COMMENT '已签收改派订单金额（人民币）（包含签收、理赔、自发头程丢件）',
    rejected_change_cny_amt     DECIMAL(24,6) COMMENT '已拒收直发订单金额（人民币）（包含拒收、客户取消、销毁）',
    refund_change_cny_amt       DECIMAL(24,6) COMMENT '已退款改派订单金额（人民币） ',
    main_sale_cnt               BIGINT        COMMENT '主商品数量',
    family_manager              STRING        COMMENT '家族负责人',
    direct_weight               DECIMAL(24,6) COMMENT '直发订单重量',
    weight                      DECIMAL(24,6) COMMENT '订单重量',
    online_order_cny_amt        DECIMAL(24,6) COMMENT '在线支付订单金额',
    cod_order_cny_amt           DECIMAL(24,6) COMMENT '到付订单金额',
    direct_online_order_cny_amt DECIMAL(24,6) COMMENT '直发在线支付订单金额',
    direct_cod_order_cny_amt    DECIMAL(24,6) COMMENT '直发到付订单金额',
    change_online_order_cny_amt DECIMAL(24,6) COMMENT '改派在线支付订单金额',
    change_cod_order_cny_amt    DECIMAL(24,6) COMMENT '改派到付订单金额',
    operat_cost                 DECIMAL(24,6) COMMENT '运营成本',
    estimate_sign_amt           DECIMAL(24,6) COMMENT '预估签收金额，特别说明：该字段仅用于统计本月及上月签收金额，另外数据以5点半之后输出数据为准',
    low_order_cny_amt           STRING        COMMENT '利润率低点金额',
    high_order_cny_amt          STRING        COMMENT '利润率高点金额',
    mid_order_cny_amt           STRING        COMMENT '利润率中点金额',
    low_sign_cny_amt            STRING        COMMENT '签收低点金额',
    high_buy_cny_amt            STRING        COMMENT '采购占比高点金额',
    hight_ad_cny_amt            STRING        COMMENT '广告占比高点金额',
    high_logistics_cny_amt      STRING        COMMENT '物流占比高点金额',
    tag                         STRING        COMMENT '投放类型',
    delete_order_cnt            STRING        COMMENT '删除订单量',
    add_cart_cnt                BIGINT        COMMENT '加购次数',
    landing_page_view           STRING        COMMENT '链接点击量',
    ad_account_id               STRING        COMMENT '广告账户id',
    ad_account_name             STRING        COMMENT '广告账户名称',
    site_name                   STRING        COMMENT '站点名称',
    online_signed_order_cnt     DECIMAL(24,6) COMMENT '在线支付已签收订单数（包含签收、理赔、自发头程丢件）',
    online_signed_cny_amt       DECIMAL(24,6) COMMENT '在线支付已签收订单金额（包含签收、理赔、自发头程丢件）',
    cod_signed_cny_amt          DECIMAL(24,6) COMMENT '货到付款已签收订单金额（包含签收、理赔、自发头程丢件）',
    cod_signed_order_cnt        DECIMAL(24,6) COMMENT '货到付款已签收订单数（包含签收、理赔、自发头程丢件）',
    estimate_online_sign_amt    DECIMAL(24,6) COMMENT '预估在线支付签收金额 特别说明：该字段仅用于统计本月及上月签收金额，另外数据以5点半之后输出数据为准',
    estimate_cod_sign_amt       DECIMAL(24,6) COMMENT '预估货到付款签收金额 特别说明：该字段仅用于统计本月及上月签收金额，另外数据以5点半之后输出数据为准'
) 
COMMENT '订单指标汇总表，保存订单相关高度汇总数据'
PARTITIONED BY
(
    pt                          STRING        COMMENT '日分区'
);
"""

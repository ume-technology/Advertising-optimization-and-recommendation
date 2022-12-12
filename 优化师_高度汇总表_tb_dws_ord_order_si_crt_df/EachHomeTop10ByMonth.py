# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:EachHomeTop10ByMonth.py
@Time:2022/11/28 17:38
@todo: 读取的是高度汇总表；拿到的是优化师的第一手相关的数据信息；可以指定平台读取目标平台各个家族的Top优化师；
       在~~每个月基于所有家族的的Top优化师s.sql~~文件中，不能准确地找到目标优化师s的投放数据；因此在这里采用这个脚本中的sql进行top优化师s相关信息的查询
~~~
wait:
    另外还有需要做的是工作是优化师舍弃或者调整的广告系列数据；做更多的广告系列数据的对比；
    读取这个数据，主要是将优化师和其产品/商品/广告系列/广告组等数据做关联，以获取优化师投放的商品的情况；~~~
"""
from functions import *

# sql = """
# select * from (
#     select family_name,opt_name,orders,ROW_NUMBER() over(partition  by family_name order by orders desc) rank from (
#     select  family_name,opt_name, sum(effect_order_cnt) orders
#     -- team_name, sum(effect_order_cnt),sum(freight+charge_fee),sum(order_cny_amt),sum(freight+charge_fee)/sum(order_cny_amt)
#     from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221101' and   pt<='20221127' and opt_name is not null
#  group by  family_name,opt_name) a
#  group by family_name,opt_name,orders) a
#  where rank<=10
# """
#
# sql = """
# select * from (
#     select family_name,opt_id,opt_name,orders,ROW_NUMBER() over(partition  by family_name order by orders desc) rank from (
#     select  family_name,opt_id,opt_name, befrom, sum(effect_order_cnt) orders
#     -- team_name, sum(effect_order_cnt),sum(freight+charge_fee),sum(order_cny_amt),sum(freight+charge_fee)/sum(order_cny_amt)
#     from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221101' and   pt<='20221127' and opt_name is not null
#  group by  family_name,opt_id,opt_name, befrom) a
#  group by family_name,opt_id,opt_name,orders) a
#  where rank<=10
# """


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

# todo 读取不同月份下，各个家族的Top5的优化师信息
sql = """
select * from (
select family_name,opt_id,opt_name,orders,ROW_NUMBER() over(partition  by family_name order by orders desc) rank from (
select  family_name,opt_id,opt_name, befrom, sum(effect_order_cnt) orders
-- team_name, sum(effect_order_cnt),sum(freight+charge_fee),sum(order_cny_amt),sum(freight+charge_fee)/sum(order_cny_amt)
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221101' and   pt<='20221130' and  befrom='facebook' -- 11月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221001' and   pt<='20221031' and  befrom='facebook' -- 10月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220901' and   pt<='20220930' and  befrom='facebook' -- 9月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220801' and   pt<='20220831' and  befrom='facebook' -- 8月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220701' and   pt<='20220731' and  befrom='facebook' -- 7月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220601' and   pt<='20220630' and  befrom='facebook' -- 6月份
  --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220501' and   pt<='20220531' and  befrom='facebook' -- 5月份
 -- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220401' and   pt<='20220430' and  befrom='facebook' -- 4月份
--from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20220301' and   pt<='20220331' and  befrom='facebook' -- 3月份
 
-- from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20211201' and   pt<='20211231' and  befrom='facebook' -- 2021 12月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20211101' and   pt<='20211130' and  befrom='facebook' -- 2021 11月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20211001' and   pt<='20211031' and  befrom='facebook' -- 2021 10月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210901' and   pt<='20210930' and  befrom='facebook' -- 2021 9月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210801' and   pt<='20210831' and  befrom='facebook' -- 2021 8月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210701' and   pt<='20210731' and  befrom='facebook' -- 2021 8月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210601' and   pt<='20210630' and  befrom='facebook' -- 2021 6月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210501' and   pt<='20210531' and  befrom='facebook' -- 2021 5月份
 --from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210401' and   pt<='20210430' and  befrom='facebook' -- 2021 4月份
 from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20210301' and   pt<='20210331' and  befrom='facebook' -- 2021 3月份
 and opt_name is not null
 group by  family_name,opt_id,opt_name, befrom) a 
 group by family_name,opt_id,opt_name,orders) a 
 where rank<=5
"""

with o.execute_sql(sql).open_reader(tunnel=True) as reader:
    all_top_apt = reader.to_pandas()

# todo 通过优化师ID找到优化师姓名
df = all_top_apt.loc[all_top_apt['opt_id'] == '2055', 'opt_name'].to_numpy()[0]

# 高度汇总表字段说明
""" 
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

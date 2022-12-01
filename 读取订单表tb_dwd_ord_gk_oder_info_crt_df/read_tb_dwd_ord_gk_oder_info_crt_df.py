# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author: UmeAI
@Blog(个人博客地址): https://www.umeai.top
@File:read_tb_dwd_ord_gk_oder_info_crt_df.py
@Time:2022/11/25 15:32
@todo: 读取订单表信息tb_dwd_ord_gk_oder_info_crt_df
       这个业务出现的原因在于处理线路上的所有信息以更好的做推荐工作：订单表信息
@fixme: 
@important:
    is_first_journey 是否头程(判断这个东西是不是已经在海外仓)
    weight 和 volume 帮助物流成本的计算；物流成本的统计：
        part-1：third_platform_fee 三方平台费用   在虾皮/亚马逊等平台产生费用
        freight as 运费(不包含手续费)
        fee13     到付服务费(手续费和其他费用) as 手续费
    pay_type_id(线下和线上) 和  payment_id(例如信用卡之类) - 之间的关系
    del_reason 和 question_reason 有问题但不一定删;
    dpe_style dpe下单类型(货物类型)  敏感单；
    is_change： 是否改派(商品是否改派标签，不作为订单是否改派的标志) ，该字段没用； use ：secondsend_status
                改派订单：User不想要了；把东西发回海外仓；已经计算了头程费用；再改派发给别人：意味着有两份尾程费用；
    giikin的仓库发货特点：
        直发：国内仓直接到用户手里；
        国内发出去：头程：国内仓到菲律宾物流公司(码头)+ 尾程：当地物流-user
        备货改派：国外仓备货，国内的商品存储一定量在国外仓；因此这个阶段形成备货物流费用：向国外仓发送备货的过程；
                备货改派这部分的费用不用关注，因为家族备货等原因家族自行会处理；
"""
from odps import ODPS
from odps import options

ACCESS_ID = 'LTAI5t6MruTBLbd9GPYaef7B'
ACCESS_KEY = 'ph8zkHlOitQkn6sIBKTgiXDPx4L6gF'
DEFAULT_PROJECT = 'cda'
END_POINT = 'http://service.cn-shenzhen.maxcompute.aliyun.com/api'

o = ODPS(ACCESS_ID, ACCESS_KEY, DEFAULT_PROJECT, endpoint=END_POINT)
options.tunnel.use_instance_tunnel = True
options.tunnel.limit_instance_tunnel = False  # 关闭limit限制，读取全部数据。

sql = """ select 
    order_id as 订单ID, is_effective_order as "是否有效订单(1 有效 0 无效)", del_reason as 删除原因, question_reason as 问题原因, 
    crt_time as 订单创建时间, ship_phone, ship_email as 邮箱, cart_info as 用户终端, 
    
    begroup as 产品所在分组,category_id as 品类ID,category_lvl1_name,category_lvl2_name,category_lvl3_name,product_id,site_id as 站点ID,
    product_name, sale_id, sale_name as 商品名称, order_amt as 订单金额,  sale_cnt as 商品数量, pay_type as 支付类型, notes as 用户备注,
    
    line_name as 线路名称, market_id as 区域ID, ship_country as 用户国家, ship_state as 省, ship_city as 市, ship_address as 街道地址,

    logistics_id as 物流渠道id, logistics_name as "物流渠道名称(包含线路及派送方式)', logistics_full_name as 物流公司全称",
    logistics_style_id as 运输方式id, logistics_style as 运输方式名称,
    
    track_status as "(默认为0,1是已签收,2是已退回) 1代表修改订单数据, 2修改订单规格, 100代表克隆订单",
    secondsend_status as "0为默认值,1被审核为二次派单,2被确认为二次派单的订单(该字段确定订单是否改派)",
    is_clone as "0代表非克隆-大于0为克隆-存放被克隆订单id",
    
    family_name, team_code, team_name, opt_id as 优化师ID, befrom as 订单来源, card_api as 广告系列ID, ad_name as 广告名称,

    weight, volume, order_price as 采购成本, operat_cost as 运营成本, third_platform_fee as 三方平台费用, freight as 运费, fee13 as 手续费

    from giikin_aliyun.tb_dwd_ord_gk_order_info_crt_df where pt=20221125;
"""

with o.execute_sql(sql).open_reader(tunnel=True) as reader:
    # todo 要多读取数据，需要多读一些分区时间；
    df = reader.to_pandas()

"""
CREATE TABLE IF NOT EXISTS tb_dwd_ord_gk_order_info_crt_df
(   
    order_price                    DECIMAL(24,6) COMMENT '采购成本',
    payed                          DECIMAL(24,6) COMMENT '收款金额',
    payed4                         DECIMAL(24,6) COMMENT '退款金额',
    finish_status                  BIGINT        COMMENT '账单回款状态 0 未回款 2 已回款 4 已退款 3 拒收 5 售后订单',
    third_platform_fee             DECIMAL(24,6) COMMENT '三方平台费用',
    freight                        DECIMAL(24,6) COMMENT '运费(不包含手续费)',
    freight1                       DECIMAL(24,6) COMMENT '头程运费',
    fee13                          DECIMAL(24,6) COMMENT '到付服务费',
    fee46                          DECIMAL(24,6) COMMENT '在线服务费',
    is_unusual_weight              BIGINT        COMMENT '重量是否异常 1：是,0：否',
    is_unusual_freight             BIGINT        COMMENT '物流运费是否异常 1：是,0：否',
    main_sale_cnt                  BIGINT        COMMENT '主商品数量',
    family_manager                 STRING        COMMENT '家族负责人',
    logistics_status               STRING        COMMENT '物流状态',
    operat_cost                    DECIMAL(24,6) COMMENT '运营成本',
    off_stock_type                 BIGINT        COMMENT '库存下架类型(1:SKU库存下架; 2:包裹下架;3:SKU库存+包裹下架;4:收货单下单;5:收货单+SKU库存下架)',
    exclud_apportion_purchase_cost DECIMAL(24,6) COMMENT '采购成本(不含海外仓储分摊费用)',
    abroad_whl_apportion_cost      DECIMAL(24,6) COMMENT '海外仓采购摊派成本',
    is_effective_order             BIGINT        COMMENT '是否有效订单(1 有效  0 无效)'

    ship_phone2                    STRING        COMMENT '运输电话',
    order_status_name              STRING        COMMENT '订单状态名称',
    logistics_fee                  DECIMAL(24,6) COMMENT '物流费用(生产源数据表中数据，暂时作废)',
    coupon                         DECIMAL(24,6) COMMENT '优惠券',
    service_fee                    DECIMAL(24,6) COMMENT '服务费',
    discount                       DECIMAL(24,6) COMMENT '折扣',
    remote_fee                     DECIMAL(24,6) COMMENT '偏远费',
    consumption_tax                DECIMAL(24,6) COMMENT '消费税',
    user_agent                     STRING        COMMENT 'ua',
    pay_domain                     STRING        COMMENT '支付域名',
    dpe_style                      STRING        COMMENT 'dpe下单类型(货物类型)',
    logistics_control              BIGINT        COMMENT '物流匹配方式',
    api_create                     BIGINT        COMMENT '这个订单是否api下单到物流公司【0否1是】',
    fbclid                         STRING        COMMENT 'fbclid',
    import_weight                  DECIMAL(24,6) COMMENT '物流导入重量',
    ship_address2                  STRING        COMMENT '第二地址',
    sale_name                      STRING        COMMENT '商品名称',
    show_type                      STRING        COMMENT '显示类型',
    show_name                      STRING        COMMENT '显示类型名称',
    chooser_id                     STRING        COMMENT '选品id',
    recommener                     STRING        COMMENT '推荐人',
    designer_id                    STRING        COMMENT '设计师id',
    product_id                     STRING        COMMENT '产品id',
    product_name                   STRING        COMMENT '产品名称',
    is_test_product                STRING        COMMENT '是否测品',
    is_change                      STRING        COMMENT '是否改派(商品是否改派标签，不作为订单是否改派的标志)',
    low_price                      STRING        COMMENT '是否低价',
    buyer_id                       STRING        COMMENT '采购员id',
    category_id                    STRING        COMMENT '品类id',
    category_lvl1_name             STRING        COMMENT '一级分类名称',
    category_lvl2_name             STRING        COMMENT '二级分类名称',
    category_lvl3_name             STRING        COMMENT '三级分类名称',
    logistics_status_id            BIGINT        COMMENT '物流状态id',
    mobile                         STRING        COMMENT '移动标示',
    browser                        STRING        COMMENT '浏览器',
    platform                       STRING        COMMENT '系统平台',
    os                             STRING        COMMENT '操作系统',
    engine                         STRING        COMMENT '浏览器引擎',
    version                        STRING        COMMENT '版本',
    engineversion                  STRING        COMMENT '引擎版本',
    order_cny_amt                  DECIMAL(24,6) COMMENT '订单金额(人民币)',
    befrom_old                     STRING        COMMENT '订单来源(原数据)',
    cod_or_online                  STRING        COMMENT '货到付款或者在线支付',
    ad_name                        STRING        COMMENT '广告名称',
    adtype                         BIGINT        COMMENT '广告类型(1:广告系列；2：广告组；3：广告)',
    tag                            BIGINT        COMMENT '投放类型(1:商品；2：分类；3：活动；4：站点)',
    site_type                      STRING        COMMENT '网站类型【1自建2复制3克隆4简站】',
    is_first_journey               BIGINT        COMMENT '是否头程',

    order_id                       BIGINT        COMMENT '订单id',
    crt_time                       STRING        COMMENT '创建时间',
    bill_number                    STRING        COMMENT '账单号',
    order_number                   STRING        COMMENT '订单号',
    waybill_number                 STRING        COMMENT '运单号',
    pay_type_id                    BIGINT        COMMENT '支付类型',
    pay_type                       STRING        COMMENT '支付类型',
    order_status_id                BIGINT        COMMENT '订单状态id',
    market_id                      BIGINT        COMMENT '区域id',
    lang_id                        BIGINT        COMMENT '语种id',
    lang_name                      STRING        COMMENT '语种名称',
    currency_id                    BIGINT        COMMENT '币种id',
    currency_name                  STRING        COMMENT '币种名称',
    line_code                      STRING        COMMENT '线路编码',
    line_name                      STRING        COMMENT '线路名称',
    currency_lang_id               BIGINT        COMMENT '币种+语种',
    team_code                      BIGINT        COMMENT '团队id',
    team_name                      STRING        COMMENT '团队名称',
    family_name                    STRING        COMMENT '家族名称',
    company                        STRING        COMMENT '分公司名称',
    org_code                       STRING        COMMENT '组织机构',
    org_name                       STRING        COMMENT '组织名称',
    order_amt                      DECIMAL(24,4) COMMENT '订单金额',
    sale_cnt                       BIGINT        COMMENT '商品数量',
    cust_id                        BIGINT        COMMENT '用户id',
    site_id                        BIGINT        COMMENT '站点id',
    site_name                      STRING        COMMENT '站点名称',
    sale_id                        BIGINT        COMMENT '商品id',
    opt_id                         BIGINT        COMMENT '优化师id',
    payment_id                     BIGINT        COMMENT '支付方式id',
    userid                         BIGINT        COMMENT '操作者id',
    logistics_id                   BIGINT        COMMENT '物流渠道id',
    logistics_name                 STRING        COMMENT '物流渠道名称(包含线路及派送方式)',
    logistics_full_name            STRING        COMMENT '物流公司全称',
    logistics_simple_name          STRING        COMMENT '物流公司简称',
    track_status                   BIGINT        COMMENT '(默认为0,1是已签收,2是已退回) 1代表修改订单数据，2修改订单规格，100代表克隆订单',
    secondsend_status              BIGINT        COMMENT '0为默认值,1被审核为二次派单,2被确认为二次派单的订单(该字段确定订单是否改派)',
    resend                         BIGINT        COMMENT '重新发货：0正常，1为重新发货，2重新发货已出库',
    lowerstatus                    BIGINT        COMMENT '是否下架，1已下架',
    is_clone                       BIGINT        COMMENT '0代表非克隆。大于0为克隆，存放被克隆订单id',
    transfer_status                BIGINT        COMMENT '是否走分仓。1代表不走分仓，0走分仓',
    logistics_status_a             BIGINT        COMMENT '作废字段',
    weight                         DECIMAL(24,4) COMMENT '重量',
    volume                         DECIMAL(24,4) COMMENT '体积',
    logistics_style_id             STRING        COMMENT '运输方式id',
    logistics_style                STRING        COMMENT '运输方式名称',
    ship_name                      STRING        COMMENT '邮寄用户名',
    ship_lastname                  STRING        COMMENT '邮寄用户姓',
    ship_phone                     STRING        COMMENT '邮寄电话',
    ship_country                   STRING        COMMENT '邮寄国家',
    ship_state                     STRING        COMMENT '邮寄省',
    ship_city                      STRING        COMMENT '邮寄市',
    ship_address                   STRING        COMMENT '邮寄地址',
    ship_zip                       STRING        COMMENT '邮寄编码',
    ship_email                     STRING        COMMENT '邮寄邮箱',
    ip                             STRING        COMMENT 'ip',
    notes                          STRING        COMMENT '用户留言',
    begroup                        STRING        COMMENT '产品所在分组',
    flag                           STRING        COMMENT '状态标记',
    befrom                         STRING        COMMENT '订单来源',
    card_api                       STRING        COMMENT '广告系列id',
    auto_verify                    STRING        COMMENT '自动审核',
    del_reason_id                  STRING        COMMENT '删除原因',
    del_reason                     STRING        COMMENT '删除原因',
    question_reason_id             STRING        COMMENT '问题原因',
    question_reason                STRING        COMMENT '问题原因',
    card_no                        STRING        COMMENT '卡号',
    update_time                    STRING        COMMENT '更新时间',
    verify_time                    STRING        COMMENT '审核时间',
    transfer_time                  STRING        COMMENT '转采购时间',
    delivery_time                  STRING        COMMENT '发货时间',
    finishtime                     STRING        COMMENT '完成时间',
    logistics_update_time          STRING        COMMENT '物流状态修改时间',
    del_time                       STRING        COMMENT '删除时间',
    remark                         STRING        COMMENT '备注',
    cart_info                      STRING        COMMENT '终端设备类型【WAP  PC】或者其他自定义值',

) 
COMMENT '订单表-按照创建时间存储'
PARTITIONED BY
(
    pt                             STRING        COMMENT '天分区'
);

"""

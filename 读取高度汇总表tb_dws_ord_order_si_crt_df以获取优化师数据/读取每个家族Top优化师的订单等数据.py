# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author: UmeAI
@Blog(个人博客地址): https://www.umeai.top
@File:读取每个家族Top优化师的订单等数据.py
@Time:2022/11/29 09:50
@todo: 读取这个数据，主要是为了和商品/产品数据做关联，以获取优化师投放的商品的情况
@fixme: 另外还有需要做的是工作是优化师舍弃或者调整的广告犀利数据；做更多的广告系列数据的对比；
@important: 
"""

sql = """
select * from (
    select family_name,opt_name,orders,ROW_NUMBER() over(partition  by family_name order by orders desc) rank from (
    select  family_name,opt_name, sum(effect_order_cnt) orders
    -- team_name, sum(effect_order_cnt),sum(freight+charge_fee),sum(order_cny_amt),sum(freight+charge_fee)/sum(order_cny_amt)
    from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221101' and   pt<='20221127' and opt_name is not null
 group by  family_name,opt_name) a 
 group by family_name,opt_name,orders) a 
 where rank<=10
"""

sql = """
select * from (
    select family_name,opt_id,opt_name,orders,ROW_NUMBER() over(partition  by family_name order by orders desc) rank from (
    select  family_name,opt_id,opt_name, befrom, sum(effect_order_cnt) orders
    -- team_name, sum(effect_order_cnt),sum(freight+charge_fee),sum(order_cny_amt),sum(freight+charge_fee)/sum(order_cny_amt)
    from giikin_aliyun.tb_dws_ord_order_si_crt_df  where pt>='20221101' and   pt<='20221127' and opt_name is not null
 group by  family_name,opt_id,opt_name, befrom) a 
 group by family_name,opt_id,opt_name,orders) a 
 where rank<=10
"""

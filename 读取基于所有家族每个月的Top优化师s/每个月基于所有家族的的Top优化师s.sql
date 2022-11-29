todo 优秀选品人员ID信息; 标准的产品、商品、选品、优化、订单量数据信息;月度统计选品人员信息
(
select
family_id,family_name,opt_id id,ifnull(opt_name,'-') as staff_name,is_contin_profit_3md,sum(contin_profit_amt_3md) contin_profit_amt_3md,'优化' position
-- staff_name,opt_name,contin_profit_amt_rank_3md
from  maxcompute.tb_rp_pub_talent_map_staff_mi
where 1=1
-- 查询当月
-- and (statis_date=DATE_SUB(CURDATE(),INTERVAL 1 day) )
-- 查询指定月份，则更改为月末
and (statis_date='2022-09-30')
and position='优化师'
and avg_effect_ord_cnt_md>0
and opt_name<>''
and opt_id>0
and is_contin_profit_3md=1
and ( is_mideast_staff<>1 or is_mideast_staff is null)
group by family_id,family_name,opt_id,opt_name,is_contin_profit_3md
order by sum(contin_profit_amt_3md) desc limit 10
)
union all
(
select
family_id,family_name,chooser_id id,ifnull(chooser_name,'-') staff_name,is_contin_profit_3md,sum(contin_profit_amt_3md) contin_profit_amt_3md,'选品' position
-- staff_name,opt_name,contin_profit_amt_rank_3md
from  maxcompute.tb_rp_pub_talent_map_staff_mi
where 1=1
and (statis_date=DATE_SUB(CURDATE(),INTERVAL 1 day) )
and position='选品师'
and avg_effect_ord_cnt_md>0
and chooser_name<>''
-- and family_id=1011
and chooser_id>0
 and is_contin_profit_3md=1
and ( is_mideast_staff<>1 or is_mideast_staff is null)
group by family_id,family_name,chooser_id,chooser_name,is_contin_profit_3md
order by sum(contin_profit_amt_3md) desc
limit 10
)

wait
    通过上述过程得到如下结果信息：看到了完全一样的商品，但是广告不同，然后产生的效果也不一样的情况。关于订单信息产生的地区信息的数据没有处理。
    # 精灵家族
        9:32970  32316  631616  32949
        8 32970  32316  1704  32949  631616
        7 1704  32970  32316  631616
        6 631616  1704  631760
        5 631616  32748  32316  1704  132
        4 1704  32748  631616  32316  32949
        3 32993  1704  32254  32949
        1 32254  32970  1704  32260
    # 神龙家族
        9:32359  128  4257
        8 32100  32359
        7 128  32100  10370  32359  631760
        6 128  32359  32100  631217  1476
        5 32359  1476  128  10155
        4 32359  1476  10155
        3 32359  1476  10155  10359
        1 32359  32780  1476  10370
    # 火凤凰家族
        9:631524  631766
        8 pass
        7 pass
        6 631524
        5 pass
    # 金狮家族
        9:10230
        8 32353  32960  10230
        7 32353
        6 32353
        5 1551
        4 1551  631225
        3 1551
        1 1551  10033
    # 金鹏家族
        3 631174
    # 雪豹数据未出现
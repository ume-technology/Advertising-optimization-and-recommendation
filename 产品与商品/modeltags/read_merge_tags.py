# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:read_merge_tags.py
@Time:2022/12/9 10:09
@Read: 
"""
import pickle
import os
import csv
import json
import pickle
import pandas as pd
import logging
import numpy as np
from tqdm import tqdm

logging.basicConfig(filename='wrong_label_product.log', level=logging.INFO)

# todo - important Standard：     giikin data standard properties
with open('../goodstags/giikin_prtswithstandards_tags.pick', 'rb') as f:
    goodstags = pickle.load(f)
    # todo 融合所有的product的tags信息到goods中；fixme 这里需要明确的是一个产品被投放成了哪些个商品，分别都在哪些线路上；
    allproductlist = []
    for k, v in goodstags.items():
        each_goodsalltags = {}
        if not isinstance(k, int):
            continue
        each_goodsalltags['product_id'] = k
        each_goodsalltags.update(v)
        allproductlist.append(each_goodsalltags)

# todo 读取产品信息维表
with open('tb_dim_pro_gk_product_df.pick', 'rb') as f:
    prtclsinfo = pickle.load(f)

# todo 读取商品标签数据
# with open( '../../产品与商品/giikinnerresult/outgiikinner/giikinnertagsfile') as f:
basepath = '../../产品与商品/giikinnerresult/outgiikinner/giikinnertagsfile'  # part of nertags data demo  save to ./data-
filename = os.listdir(basepath)
allgoodsdatadf = ''
for idx, i in enumerate(filename):
    targetfile = basepath + '/' + i
    with open(targetfile, 'rb') as f:
        eachdata = pickle.load(f)
        eachdata = eachdata.dropna(subset=['product_name'])
        if idx == 0:
            allgoodsdatadf = eachdata
            continue
        allgoodsdatadf = pd.concat([allgoodsdatadf, eachdata], axis=0)

# todo 参与投放的产品
joinAD_product = []
joinAD_product_no = []
for each_prt in tqdm(allproductlist):  # todo 产品标准TAGs数据
    prtidfrom_porduct = each_prt['product_id']
    # todo 把产品维表中的tags标签融合到TAGS数据中
    res = prtclsinfo.loc[prtclsinfo['product_id'] == prtidfrom_porduct]
    if len(res) >= 1:
        for each_prtclsinfo in res.itertuples():
            each_prt['主供应商ID'] = getattr(each_prtclsinfo, 'seller_id')
            each_prt['采购员ID'] = getattr(each_prtclsinfo, 'buyer')
            each_prt['采购员NAME'] = getattr(each_prtclsinfo, 'buyer_name')
            each_prt['采购员所属部门'] = getattr(each_prtclsinfo, 'department_name')
            each_prt['选品员'] = getattr(each_prtclsinfo, 'selection')
            each_prt['采购地址'] = getattr(each_prtclsinfo, 'purchase_url')
            each_prt['是否有SKU'] = getattr(each_prtclsinfo, 'has_sku')
            each_prt['发货地'] = getattr(each_prtclsinfo, 'delivery_place')
            each_prt['产品价格'] = getattr(each_prtclsinfo, 'product_price')
            each_prt['采购价格'] = getattr(each_prtclsinfo, 'purchase_price')
            each_prt['体积'] = getattr(each_prtclsinfo, 'volume')
            each_prt['重量'] = getattr(each_prtclsinfo, 'weight')
            each_prt['产品图片'] = getattr(each_prtclsinfo, 'product_image')
            # each_prt['SP_From_Selector'] = getattr(each_prtclsinfo, 'selling_point')
            each_prt['签收率'] = getattr(each_prtclsinfo, 'sign_rate')
            # todo 产品与所属类别之间的关系
            category_lvl1_name = getattr(each_prtclsinfo, 'category_lvl1_name')
            category_lvl2_name = getattr(each_prtclsinfo, 'category_lvl2_name')
            category_lvl3_name = getattr(each_prtclsinfo, 'category_lvl3_name')
            if category_lvl1_name is not None and category_lvl2_name is not None and category_lvl3_name is not None:
                prtcls = category_lvl1_name + '\t' + category_lvl2_name + '\t' + category_lvl3_name
            else:
                prtcls = '类别信息缺失'
            each_prt['产品类别'] = prtcls
            break
    else:
        logging.warning('product tags none warning...{}'.format(prtidfrom_porduct))

    # todo 将NER tags 标签融入 TAGS 数据中
    res = allgoodsdatadf.loc[allgoodsdatadf['product_id'] == prtidfrom_porduct]  # todo ner产品标签数据
    eachprtgoodslist = []  # 产品和商品的衍生关系
    if len(res) >= 1:
        for i in res.itertuples():
            each_goods_from_prt_info = {}
            # todo 以下是商品属性
            each_goods_from_prt_info['sale_id'] = getattr(i, 'sale_id')
            each_goods_from_prt_info['platform'] = getattr(i, 'platfrom')
            each_goods_from_prt_info['team_name'] = getattr(i, 'team_name')
            each_goods_from_prt_info['line_name'] = getattr(i, 'line_name')
            each_goods_from_prt_info['designer_id'] = getattr(i, 'designer_id')
            each_goods_from_prt_info['opt_id'] = getattr(i, 'opt_id')

            each_goods_from_prt_info['ad_account_id'] = getattr(i, 'ad_account_id')
            each_goods_from_prt_info['campaign_id'] = getattr(i, 'campaign_id')
            each_goods_from_prt_info['ad_group_id'] = getattr(i, 'ad_group_id')

            each_goods_from_prt_info['product_name'] = getattr(i, 'product_name')
            each_goods_from_prt_info['genders'] = getattr(i, 'genders')
            each_goods_from_prt_info['sale_name'] = getattr(i, 'sale_name')
            each_goods_from_prt_info['ad_slogans'] = getattr(i, 'ad_slogans')
            each_goods_from_prt_info['clantext'] = getattr(i, 'cleantext')
            each_goods_from_prt_info['interest_words'] = getattr(i, 'interest_words')

            each_goods_from_prt_info['点击量'] = getattr(i, 'clicks_cnt')  # 点击量
            each_goods_from_prt_info['曝光量'] = getattr(i, 'people_cover_cnt')  # 曝光量
            each_goods_from_prt_info['点击率'] = getattr(i, 'clicks_rate')  # 点击率

            each_goods_from_prt_info['加购次数'] = getattr(i, 'add_cart_cnt')  # 加购次数
            each_goods_from_prt_info['下单数'] = getattr(i, 'order_cnt')  # 下单数
            each_goods_from_prt_info['转化率'] = getattr(i, 'conversions_rate')  # 转化率

            each_goods_from_prt_info['age_range'] = getattr(i, 'age_range')
            each_goods_from_prt_info['country'] = getattr(i, 'country')
            each_goods_from_prt_info['currency_id'] = getattr(i, 'currency_id')
            each_goods_from_prt_info['lang_id'] = getattr(i, 'lang_id')
            if len(eachprtgoodslist) >= 1:
                sale_idlist = [__.get('sale_id') for __ in eachprtgoodslist]
                if each_goods_from_prt_info.get('sale_id') not in sale_idlist:
                    eachprtgoodslist.append(each_goods_from_prt_info)
            else:
                eachprtgoodslist.append(each_goods_from_prt_info)
            # todo 获取商品关系
            personinscene = getattr(i, 'personInScene')
            if personinscene:
                each_prt['使用人群'] = [_['subject'] for _ in personinscene]
                each_prt['适用场景'] = [_['object'] for _ in personinscene]
            prtinscene = getattr(i, 'prtInScene')
            if prtinscene:
                each_prt['商品搭配'] = [_['subject'] for _ in prtinscene]
                each_prt['适用场景'] = [_['object'] for _ in prtinscene]
            prtsuitprt = getattr(i, 'prtDpPrt')
            if prtsuitprt:
                each_prt['搭配品类'] = [_['object'] for _ in prtsuitprt]
            goodsfeatures = getattr(i, 'goodsfeatures')
            if goodsfeatures:
                for k, v in goodsfeatures.items():
                    if len(v) != 0:
                        each_prt[k] = list(v)
            bre = 'breakpoint'

        each_prt['投放商品'] = eachprtgoodslist
        joinAD_product.append(each_prt)
    else:
        joinAD_product_no.append(each_prt)
        logging.warning('product no join AD ...{}'.format(prtidfrom_porduct))

# todo 存储以商品为核心的标签数据;以作为知识图谱构建的标准数据集
# with open('./data-/joinAD_product.pick', 'wb') as f:
#     pickle.dump(joinAD_product, f)  # 包含了ner tags+产品维表tags的TAGS数据
# with open('./data-/joinAD_product_no.pick', 'wb') as f:
#     pickle.dump(joinAD_product_no, f)  # 只有产品维表中的tags到TAGS中的产品数据
pass

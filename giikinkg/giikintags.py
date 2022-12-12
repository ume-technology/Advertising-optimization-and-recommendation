# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:giikintags.py
@Time:2022/12/9 15:27
@Read: 汇总抽取出来的giikin的数据标签信息; 构建 graph nodes
"""
import re
import jieba
import pickle

# from openbg500L.read500l import *

# todo 因为在构建商品标签的过程中使用这个数据时，针对产品类别的确认不合理，因此这里再读pro-dim的数据，补全这个产品类别信息
with open('../giikindataset/concattagwithnertags/tb_dim_pro_gk_product_df.pick', 'rb') as fprodim:
    prodim = pickle.load(fprodim)

with open('../giikindataset/concattagwithnertags/data/joinAD_product.pick', 'rb') as f:
    nertags_dimprotags_TAGS = pickle.load(f)
    # alltags = set()
    # for i in nertags_dimprotags_TAGS:
    #     for _ in i.keys():
    #         alltags.add(_)


# with open('../giikindataset/concattagwithnertags/data/joinAD_product_no.pick', 'rb') as f:
#     tagswithoutner = pickle.load(f)


# todo 创建 Concept
def read_nodes_base_product():
    # important    concept:里面包含子分类；但是定义concept node时体现不出来各个概念子分类的存在，子分类直接存储在spo中
    # category = []  # cat1/cat2/cat3
    scene = []  # scene/market
    crowd = []  # mom/worker/student/carer
    season = []  # season/age/day  wait day节日
    placeorbody = []  # outdoor/indoor/sky  or  eye legs ....
    brands = []

    rel_category_scene = []
    rel_category_crowd = []
    rel_category_time = []
    rel_category_placeorbody = []
    rel_category_brand = []

    product_info = []
    for eachprtwithtags in nertags_dimprotags_TAGS:
        prt_dict = {}
        prt_dict['商品卖点'] = []
        prt_dict['适配人群或对象'] = []
        prt_dict['商品材质'] = []  # 商品材料
        prt_dict['适用场地或部位'] = []
        prt_dict['商品功能'] = []
        prt_dict['应用时间'] = []
        for key in list(eachprtwithtags):
            if eachprtwithtags[key] == 'null ' or eachprtwithtags[key] == -1:
                eachprtwithtags.pop(key)
                continue
            if not eachprtwithtags[key]:
                eachprtwithtags.pop(key)
                continue
            if eachprtwithtags[key] is None:
                eachprtwithtags.pop(key)
                continue
            if isinstance(eachprtwithtags[key], int) or isinstance(eachprtwithtags[key], float):
                prt_dict[key] = eachprtwithtags.pop(key)
                continue
            if key == 'category_lvl3_name_giikin' or key == 'category_lvl3_id_giikin' or key == 'product_name_giikin' or key == '归一化产品' or key == '商品产地【虚假】':
                eachprtwithtags.pop(key)
                continue

            prt_dict[key] = eachprtwithtags.get(key)

        keys = eachprtwithtags.keys()
        # property base property
        each_prt = eachprtwithtags.get('product_name')
        if each_prt is not None:
            each_prt = eachprtwithtags.pop('product_name')
        each_id = eachprtwithtags.get('product_id')
        if each_id is not None:
            each_id = eachprtwithtags.pop('product_id')
        if each_id is None:
            each_id = prodim['product_id']
        if each_prt is None:
            each_prts = prodim.loc[prodim['product_id'] == each_id]
            each_prt = each_prts['product_name'].tolist()[0]
        prt_dict['prt_name'] = each_prt
        prt_dict['prt_id'] = each_id

        if '元素标签' in keys:
            popv = prt_dict.pop('元素标签')
            if isinstance(popv, str):
                prt_dict['商品卖点'].append(popv)
            else:
                prt_dict['商品卖点'] += set(popv)
        if '特色卖点' in keys:
            popv = prt_dict.pop('特色卖点')
            if isinstance(popv, str):
                prt_dict['商品卖点'].append(popv)
            else:
                prt_dict['商品卖点'] += set(popv)
        if '卖点提取' in keys:
            popv = prt_dict.pop('卖点提取')
            if isinstance(popv, str):
                prt_dict['商品卖点'].append(popv)
            else:
                prt_dict['商品卖点'] += set(popv)
        # property  material
        if '商品材料' in keys:
            popv = prt_dict.pop('商品材料')
            if isinstance(popv, str):
                prt_dict['商品材质'].append(popv)
            else:
                prt_dict['商品材质'] += set(popv)
        if '材质' in keys:
            popv = prt_dict.pop('材质')
            if isinstance(popv, str):
                prt_dict['商品材质'].append(popv)
            else:
                prt_dict['商品材质'] += set(popv)
        if '材质标签' in keys:
            popv = prt_dict.pop('材质标签')
            if isinstance(popv, str):
                prt_dict['商品材质'].append(popv)
            else:
                prt_dict['商品材质'] += set(popv)
        if '材质材料' in keys:
            popv = prt_dict.pop('材质材料')
            if isinstance(popv, str):
                prt_dict['商品材质'].append(popv)
            else:
                prt_dict['商品材质'] += set(popv)

        # property functions
        if '功能' in keys:
            popv = prt_dict.pop('功能')
            if isinstance(popv, str):
                prt_dict['商品功能'].append(popv)
            else:
                prt_dict['商品功能'] += set(popv)
        if '功能功效' in keys:
            popv = prt_dict.pop('功能功效')
            if isinstance(popv, str):
                prt_dict['商品功能'].append(popv)
            else:
                prt_dict['商品功能'] += set(popv)
        if '功能标签' in keys:
            popv = prt_dict.pop('功能标签')
            if isinstance(popv, str):
                prt_dict['商品功能'].append(popv)
            else:
                prt_dict['商品功能'] += set(popv)
        if '功能类型' in keys:
            popv = prt_dict.pop('功能类型')
            if isinstance(popv, str):
                prt_dict['商品功能'].append(popv)
            else:
                prt_dict['商品功能'] += set(popv)

        # property dp
        if '商品搭配' in keys:
            prt_dict['商品搭配'] = set(prt_dict.pop('商品搭配'))
        # property pl
        if '搭配品类' in keys:
            prt_dict['搭配品类'] = set(prt_dict.pop('搭配品类'))
        # =============================================================================================================
        # node category
        # categorys = eachprtwithtags.pop('产品类别').split('\t')
        # category += categorys[-1]

        # node scene
        if '适用场景' in keys:
            scene += set(eachprtwithtags.get('适用场景'))
            prt_dict['适用场景'] = set(eachprtwithtags.pop('适用场景'))
            for i in set(prt_dict.get('适用场景')):
                rel_category_scene.append([each_prt, i])

        # node crowd
        if '适用人群' in keys:
            popv = prt_dict.pop('适用人群')
            if isinstance(popv, str):
                prt_dict['适配人群或对象'].append(popv)
                crowd.append(popv)
            else:
                prt_dict['适配人群或对象'] += set(popv)
                crowd += set(popv)
        if '人群标签' in keys:
            popv = prt_dict.pop('人群标签')
            if isinstance(popv, str):
                prt_dict['适配人群或对象'].append(popv)
                crowd.append(popv)
            else:
                prt_dict['适配人群或对象'] += set(popv)
                crowd += set(popv)
        if '使用人群' in keys:
            popv = prt_dict.pop('使用人群')
            if isinstance(popv, str):
                prt_dict['适配人群或对象'].append(popv)
                crowd.append(popv)
            else:
                prt_dict['适配人群或对象'] += set(popv)
                crowd += set(popv)
        if '商品适用对象' in keys:
            popv = prt_dict.pop('商品适用对象')
            if isinstance(popv, str):
                prt_dict['适配人群或对象'].append(popv)
                crowd.append(popv)
            else:
                prt_dict['适配人群或对象'] += set(popv)
                crowd += set(popv)
        for i in set(prt_dict['适配人群或对象']):
            rel_category_crowd.append([each_prt, i])

        # node season
        if '季节标签' in keys:
            popv = eachprtwithtags.pop('季节标签')
            if isinstance(popv, str):
                prt_dict['应用时间'].append(popv)
                season.append(popv)
            else:
                prt_dict['应用时间'] += set(popv)
                season += set(popv)
        if '适合季节' in keys:
            popv = eachprtwithtags.pop('适合季节')
            if isinstance(popv, str):
                prt_dict['应用时间'].append(popv)
                season.append(popv)
            else:
                prt_dict['应用时间'] += set(popv)
                season += set(popv)
        if '适用年龄段' in keys:
            popv = eachprtwithtags.pop('适用年龄段')
            if isinstance(popv, str):
                prt_dict['应用时间'].append(popv)
                season.append(popv)
            else:
                prt_dict['应用时间'] += set(popv)
                season += set(popv)
        for i in set(prt_dict.get('应用时间')):
            rel_category_time.append([each_prt, i])

        # node brands
        if '品牌' in keys:
            brand = eachprtwithtags.pop('品牌')
            brands.append(brand)
            rel_category_brand.append([each_prt, brand])

        # node suitable
        # if '搭配品类' in keys:
        #     suitable += set(eachprtwithtags.pop('搭配品类'))

        # node location
        if '适用地点与场景' in keys:
            prt_dict['适用场地或部位'] += set(prt_dict.pop('适用地点与场景'))
            placeorbody += set(eachprtwithtags.get('适用地点与场景'))
            for i in set(eachprtwithtags.pop('适用地点与场景')):
                rel_category_placeorbody.append([each_prt, i])
        if '适用部位' in keys:
            prt_dict['适用场地或部位'] += set(prt_dict.pop('适用部位'))
            placeorbody += set(eachprtwithtags.get('适用部位'))
            for i in set(eachprtwithtags.pop('适用部位')):
                rel_category_placeorbody.append([each_prt, i])

        product_info.append(prt_dict)
    return scene, crowd, season, placeorbody, brands, product_info, rel_category_scene, rel_category_crowd, rel_category_time, rel_category_placeorbody, rel_category_brand


read_nodes_base_product()

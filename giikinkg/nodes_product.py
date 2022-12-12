# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author: UmeAI
@Blog(个人博客地址): https://www.umeai.top
@File:nodes_product.py
@Time:2022/12/2 10:54
@todo: 构建广告库中产品节点数据
@fixme: 
@important: 
"""
import json
import pickle


def product_nodes():
    """
    1 - 产品库的产品具备的属性；
    2 - 广告库的产品具备的属性；
    :return:
    """
    # goods_ids = []  # 将商品作为其他本体 - 商品具备完全体的商品属性
    # pro_clss = []  # 类和类之间会产品存在搭配关系 - 因此作为本体
    # # todo 标准本体具备的属性
    # goods_types = []  # 不再具备任何属性和关系
    # opt_ids = []  # 不具备任何属性
    # each_goods_prices = []  # 商品客单价
    # off_sign_rates = []
    # on_sign_rates = []
    # ad_cost_goodscampaigns = []  # 某个商品对应的广告系列的花费
    # fit_rates = []
    # rois = []

    prt_infos = []
    # =====================================================================================
    # 本体确认
    pro_names = []  # 代表了某个产品，具备完全体的产品属性，因此作为本体
    coloryuansu_tags = []
    size_tags = []  # 尺寸
    group_tags = []  # 适用人群
    material_tags = []  # 材质材料
    design_tags = []  # 设计款式
    function_tags = []  # 功能
    fengge_tags = []  # 风格
    sellpoint_tags = []  # 卖点特色
    usetime_tags = []  # 适用季节
    age_tags = []  # 适用年龄
    selltime_tags = []  # 上市时间
    cat3_tags = []  # 所属类别
    dpgoods_tags = []  # 商品搭配
    usescene_tags = []  # 应用场景
    dpcls_tags = []  # 品类搭配
    pubgoods_tags = []  # 产品投放出去的商品
    body_tags = []  # 适用部位
    eat_tags = []  # 食用方法
    loc_tags = []  # 适用地点与场景
    bof_tags = []  # 触感
    tas_tags = []  # 口味
    # 关系确认
    rels_prt_coloryuansu = []  # 颜色元素：比如纯色；但是有白色/红色等颜色，不是一个本体
    rels_prt_size = []
    rels_prt_groups = []
    rels_prt_material = []
    rels_prt_functions = []
    rels_prt_design = []
    rels_prt_fengge = []
    rels_prt_sellpoint = []
    rels_prt_usetime = []
    rels_prt_age = []
    rels_prt_selltime = []
    rels_prt_cat3 = []
    rels_prt_dpgoods = []
    rels_prt_usescene = []
    rels_prt_dpcls = []
    rels_prt_pubgoods = []
    rels_prt_body = []
    rels_prt_eat = []
    rels_prt_loc = []
    rels_prt_bof = []
    rels_prt_tas = []
    # todo 因为在构建商品标签的过程中使用这个数据时，针对产品类别的确认不合理，因此这里再读pro-dim的数据，补全这个产品类别信息
    with open('../giikindataset/concattagwithnertags/tb_dim_pro_gk_product_df.pick', 'rb') as fprodim:
        prodim = pickle.load(fprodim)

    # todo 以【产品维表】为核心扩展出来的所有tags的TAGS数据（包括NER数据）
    with open('../融合产品维表tags和nertags以及两者数据的concat/extractrelationship/data/joinAD_product.pick', 'rb') as f:
        nertags_dimprotags_TAGS = pickle.load(f)
        for eachprtwithtags in nertags_dimprotags_TAGS:
            prt_dict = {}
            each_prt = eachprtwithtags.get('product_name')
            if each_prt is None:
                each_prts = eachprtwithtags.get('product_id')
                each_prts = prodim.loc[prodim['product_id'] == each_prts]
                each_prt = each_prts['product_name'].tolist()[0]
            # pro_names.append(each_prt)
            # for eachtag in alltags:
            #     prt_dict[eachtag] = ''
            # 商品属性创建
            for k, v in eachprtwithtags.items():
                # 添加属性
                prt_dict[k] = v
                # 添加关系
                if k == '元素标签':
                    coloryuansu_tags.append(v)
                    rels_prt_coloryuansu.append([each_prt, v])
                if k == '尺寸标签':
                    size_tags.append(v)
                    rels_prt_size.append([each_prt, v])
                if k == '人群标签':
                    if isinstance(v, str):
                        group_tags.append(v)
                        rels_prt_groups.append([each_prt, v])
                    if isinstance(v, list):
                        group_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_groups.append([each_prt, __])
                    # group_tags.append(v)
                    # rels_prt_groups.append([each_prt, v])
                if k == '适用人群':
                    if isinstance(v, str):
                        group_tags.append(v)
                        rels_prt_groups.append([each_prt, v])
                    if isinstance(v, list):
                        group_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_groups.append([each_prt, __])
                    # group_tags.append(v)
                    # rels_prt_groups.append([each_prt, v])
                if k == '商品适用对象':
                    if isinstance(v, str):
                        group_tags.append(v)
                        rels_prt_groups.append([each_prt, v])
                    if isinstance(v, list):
                        group_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_groups.append([each_prt, __])
                    # group_tags += list(set(v))
                    # rels_prt_groups.append([each_prt, v])
                if k == '面料名称':
                    if isinstance(v, str):
                        material_tags.append(v)
                        rels_prt_material.append([each_prt, v])
                    if isinstance(v, list):
                        material_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_material.append([each_prt, __])
                    # material_tags.append(v)
                    # rels_prt_material.append([each_prt, v])
                if k == '材质材料':
                    if isinstance(v, str):
                        material_tags.append(v)
                        rels_prt_material.append([each_prt, v])
                    if isinstance(v, list):
                        material_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_material.append([each_prt, __])
                    # material_tags += list(set(v))
                    # rels_prt_material.append([each_prt, v])
                if k == '功能':
                    if isinstance(v, str):
                        function_tags.append(v)
                        rels_prt_functions.append([each_prt, v])
                    if isinstance(v, list):
                        function_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_functions.append([each_prt, __])
                    # function_tags.append(v)
                    # rels_prt_functions.append([each_prt, v])
                if k == '功能功效':
                    function_tags += list(set(v))
                    for __ in list(set(v)):
                        rels_prt_functions.append([each_prt, __])
                if k == '适用地点与场景':
                    loc_tags += list(set(v))
                    for __ in v:
                        rels_prt_loc.append([each_prt, __])
                if k == '款式':
                    design_tags.append(v)
                    rels_prt_design.append([each_prt, v])
                if k == '风格':
                    fengge_tags.append(v)
                    rels_prt_fengge.append([each_prt, v])
                if k == '设计特色':
                    sellpoint_tags.append(v)
                    rels_prt_sellpoint.append([each_prt, v])
                if k == '特色卖点':
                    sellpoint_tags += v
                    for __ in list(set(v)):
                        rels_prt_sellpoint.append([each_prt, __])
                if k == '适合季节':
                    if isinstance(v, str):
                        usetime_tags.append(v)
                        rels_prt_usetime.append([each_prt, v])
                    if isinstance(v, list):
                        usetime_tags += list(set(v))
                        for __ in list(set(v)):
                            if isinstance(__, str):
                                rels_prt_usetime.append([each_prt, __])
                            if isinstance(__, list):
                                for ___ in list(set(__)):
                                    rels_prt_usetime.append([each_prt, ___])
                    # usetime_tags.append(v)
                    # rels_prt_usetime.append([each_prt, v])
                if k == '适用时间':
                    usetime_tags += list(set(v))
                    rels_prt_usetime.append([each_prt, v])
                if k == '适用年龄段':
                    age_tags.append(v)
                    rels_prt_age.append([each_prt, v])
                if k == '适配年龄':
                    if isinstance(v, str):
                        age_tags.append(v)
                        rels_prt_age.append([each_prt, v])
                    if isinstance(v, list):
                        age_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_age.append([each_prt, __])
                    # age_tags += list(set(v))
                    # rels_prt_age.append([each_prt, v])
                if k == '上市时间':
                    if isinstance(v, str):
                        selltime_tags.append(v)
                        rels_prt_selltime.append([each_prt, v])
                    if isinstance(v, list):
                        selltime_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_selltime.append([each_prt, __])
                    # selltime_tags.append(v)
                    # rels_prt_selltime.append([each_prt, v])
                if k == '产品类别':  # ['']
                    if v == '类别信息缺失':
                        cate3_tag = each_prts['category_lvl3_name'].tolist()[0]
                        cate2_tag = each_prts['category_lvl2_name'].tolist()[0]
                        cat3_tags += [cate2_tag, cate3_tag]
                        rels_prt_cat3.append([each_prt, cat3_tags])
                    else:
                        cats = v.split('\t')
                        cat3_tags += [cats[1], cats[2]]
                        if eachprtwithtags.get('归一化产品'):
                            rels_prt_cat3.append([each_prt, eachprtwithtags.get('归一化产品')[0]])
                        else:
                            rels_prt_cat3.append([each_prt, cats[2]])
                if k == '商品搭配':
                    dpgoods_tags += list(set(v))
                    for __ in list(set(v)):
                        rels_prt_dpgoods.append([each_prt, __])
                if k == '适用场景':
                    usescene_tags += list(set(v))
                    for __ in list(set(v)):
                        rels_prt_usescene.append([each_prt, __])
                if k == '搭配品类':
                    dpcls_tags += list(set(v))
                    for __ in list(set(v)):
                        rels_prt_dpcls.append([each_prt, __])
                if k == '适用部位':
                    body_tags += list(set(v))
                    for __ in list(set(v)):
                        rels_prt_body.append([each_prt, __])
                if k == '投放商品':
                    pubgoods_tags += v
                    for __ in v:
                        rels_prt_pubgoods.append([each_prt, __])
                if k == '食用方法':
                    if isinstance(v, str):
                        eat_tags.append(v)
                        rels_prt_eat.append([each_prt, v])
                    if isinstance(v, list):
                        eat_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_eat.append([each_prt, __])
                if k == '触感':
                    bof_tags += list(set(v))
                    for __ in list(set(v)):
                        rels_prt_bof.append([each_prt, __])
                if k == '口味':
                    if isinstance(v, str):
                        tas_tags.append(v)
                        rels_prt_tas.append([each_prt, v])
                    if isinstance(v, list):
                        tas_tags += list(set(v))
                        for __ in list(set(v)):
                            rels_prt_tas.append([each_prt, __])
            prt_infos.append(prt_dict)

    # todo 产品投放后具备的相关指标
    # 从广告库拉取的产品数据_columns = gk_product_reporter_df.columns
    # tmp = gk_product_reporter_df.head(50)
    # for tup in tmp.itertuples():
    #     pro_ids.append(getattr(tup, '产品ID'))
    #     goods_ids.append(getattr(tup, '商品ID'))
    #     pro_clss.append(getattr(tup, '产品分类'))
    #     goods_types.append(getattr(tup, '商品类型'))
    #     opt_ids.append(getattr(tup, 'ader_id'))
    #     each_goods_prices.append(getattr(tup, '客单价'))
    #     off_sign_rates.append(getattr(tup, '到付签收率'))
    #     on_sign_rates.append(getattr(tup, '在线支付签收率'))
    #     ad_cost_goodscampaigns.append(getattr(tup, 'ad_cost'))
    #     fit_rates.append(getattr(tup, '利润率'))
    #     rois.append(getattr(tup, 'roi'))

    bre = 'break'

    return set(pro_names), set(coloryuansu_tags), set(size_tags), set(group_tags), set(material_tags), set(design_tags), \
           set(function_tags), set(fengge_tags), set(sellpoint_tags), set(usetime_tags), set(age_tags), \
           set(selltime_tags), set(cat3_tags), set(dpgoods_tags), set(usescene_tags), set(dpcls_tags), set(body_tags), set(eat_tags), set(loc_tags), set(bof_tags), set(tas_tags), \
           pubgoods_tags, prt_infos, rels_prt_coloryuansu, rels_prt_size, rels_prt_groups, rels_prt_material, \
           rels_prt_functions, rels_prt_design, rels_prt_fengge, rels_prt_sellpoint, rels_prt_usetime, rels_prt_age, \
           rels_prt_selltime, rels_prt_cat3, rels_prt_dpgoods, rels_prt_usescene, rels_prt_dpcls, rels_prt_pubgoods, rels_prt_body, rels_prt_eat, rels_prt_loc, rels_prt_bof, rels_prt_tas


def write_node_json(label, nodes):
    count = 0
    en_list = []
    for node in nodes:  # 将结点集中的结点设置为
        item = {}
        item['label'] = label
        if label == 'Products':
            for k, v in node.items():
                item[k] = v
        else:
            item['name'] = node
        count += 1
        # print(count, len(nodes))
        en_list.append(item)
    print(label, count)
    return en_list


def create_nodes_json():
    pro_names, coloryuansu_tags, size_tags, group_tags, material_tags, design_tags, function_tags, fengge_tags, sellpoint_tags, \
    usetime_tags, age_tags, selltime_tags, cat3_tags, dpgoods_tags, usescene_tags, dpcls_tags, body_tags, eat_tags, loc_tags, bof_tags, tas_tags, pubgoods_tags, \
    prt_infos, rels_prt_coloryuansu, rels_prt_size, rels_prt_groups, rels_prt_material, rels_prt_functions, rels_prt_design, rels_prt_fengge, rels_prt_sellpoint, rels_prt_usetime, rels_prt_age, \
    rels_prt_selltime, rels_prt_cat3, rels_prt_dpgoods, rels_prt_usescene, rels_prt_dpcls, rels_prt_pubgoods, rels_prt_body, rels_prt_eat, rels_prt_loc, rels_prt_bof, rels_prt_tas = product_nodes()
    entities = []
    l = write_node_json('Products', prt_infos)
    entities.extend(l)
    l = write_node_json('pro_names', pro_names)
    entities.extend(l)
    l = write_node_json('coloryuansu_tags', coloryuansu_tags)
    entities.extend(l)
    l = write_node_json('size_tags', size_tags)
    entities.extend(l)
    l = write_node_json('material_tags', material_tags)
    entities.extend(l)
    l = write_node_json('design_tags', design_tags)
    entities.extend(l)
    l = write_node_json('function_tags', function_tags)
    entities.extend(l)
    l = write_node_json('fengge_tags', fengge_tags)
    entities.extend(l)
    l = write_node_json('sellpoint_tags', sellpoint_tags)
    entities.extend(l)
    l = write_node_json('usetime_tags', usetime_tags)
    entities.extend(l)
    l = write_node_json('age_tags', age_tags)
    entities.extend(l)
    l = write_node_json('selltime_tags', selltime_tags)
    entities.extend(l)
    l = write_node_json('cat3_tags', cat3_tags)
    entities.extend(l)
    l = write_node_json('dpgoods_tags', dpgoods_tags)
    entities.extend(l)
    l = write_node_json('usescene_tags', usescene_tags)
    entities.extend(l)
    l = write_node_json('dpcls_tags', dpcls_tags)
    entities.extend(l)
    l = write_node_json('body_tags', body_tags)
    entities.extend(l)
    l = write_node_json('eat_tags', eat_tags)
    entities.extend(l)
    l = write_node_json('loc_tags', loc_tags)
    entities.extend(l)
    l = write_node_json('bof_tags', bof_tags)
    entities.extend(l)
    l = write_node_json('tas_tags', tas_tags)
    entities.extend(l)
    l = write_node_json('pubgoods_tags', pubgoods_tags)
    entities.extend(l)
    filename = './graph_node_data/entities.pick'
    # json.dump(entities, open(filename, 'w', encoding='utf-8'), indent=4, ensure_ascii=False)
    with open(filename, 'wb') as f:
        pickle.dump(entities, f)
    return


def write_rel_json(start_node, end_node, edges, rel_type, rel_name, postfix=None):
    """
    创建实体关联边
    :param start_node:
    :param end_node:
    :param edges:
    :param rel_type:
    :param rel_name:
    :param postfix:
    :return:
    """
    # todo 创建图谱的【实体+边+实体】的框架
    rel_set = {}
    rel_set['start_entity_type'] = start_node
    rel_set['end_entity_type'] = end_node
    rel_set['rel_type'] = rel_type
    rel_set['rel_name'] = rel_name
    # todo 把具体的【实体+边+实体】填入上述框架中
    set_edges = []
    for edge in edges:
        if isinstance(edge[1], list):
            print('1:', edge)
            set_edges.append(edge[0] + '###' + edge[1][0])
        if isinstance(edge[1], dict):
            print('2:', edge)
            # set_edges.append('###'.join(json.dumps(edge[1])))  # TypeError: Object of type Decimal is not JSON serializable
            set_edges.append('###'.join(str(edge[1])))
        else:
            print('3:', edge)
            set_edges.append('###'.join(edge))
        # set_edges.append(edge[0] + '###' + edge[1][0])
    all = len(set(set_edges))
    if postfix is None:
        filename = './graph_node_data/' + rel_type + '.pick'
    else:
        filename = './graph_node_data/' + rel_type + postfix + '.pick'
    rels = []
    count = 0
    for edge in set(set_edges):
        edge = edge.split('###')
        p = edge[0]
        q = edge[1]
        item = {}
        item['start_entity_name'] = p
        item['end_entity_name'] = q
        count += 1
        rels.append(item)
    rel_set['rels'] = rels
    print(rel_type, all)
    # json.dump(rel_set, open(filename, 'w', encoding='utf-8'), indent=4, ensure_ascii=False)
    with open(filename, 'wb') as f:
        pickle.dump(rel_set, f)
    return rel_set


def create_rels_nodes():
    pro_names, coloryuansu_tags, size_tags, group_tags, material_tags, design_tags, function_tags, fengge_tags, sellpoint_tags, \
    usetime_tags, age_tags, selltime_tags, cat3_tags, dpgoods_tags, usescene_tags, dpcls_tags, body_tags, eat_tags, loc_tags, bof_tags, tas_tags, pubgoods_tags, \
    prt_infos, rels_prt_coloryuansu, rels_prt_size, rels_prt_groups, rels_prt_material, rels_prt_functions, rels_prt_design, rels_prt_fengge, rels_prt_sellpoint, rels_prt_usetime, rels_prt_age, \
    rels_prt_selltime, rels_prt_cat3, rels_prt_dpgoods, rels_prt_usescene, rels_prt_dpcls, rels_prt_pubgoods, rels_prt_body, rels_prt_eat, rels_prt_loc, rels_prt_bof, rels_prt_tas = product_nodes()
    relations = []
    # rel_set = write_rel_json('Product', 'coloryuansu', rels_prt_coloryuansu, 'rels_prt_coloryuansu', '颜色元素')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'size', rels_prt_size, 'rels_prt_size', '尺寸规格')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'groups', rels_prt_groups, 'rels_prt_groups', '适用对象')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'material', rels_prt_material, 'rels_prt_material', '材质材料')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'function', rels_prt_functions, 'rels_prt_functions', '功能功效')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'design', rels_prt_design, 'rels_prt_design', '款式设计')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'style', rels_prt_fengge, 'rels_prt_fengge', '流行风格')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'sellpoint', rels_prt_sellpoint, 'rels_prt_sellpoint', '卖点')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'usetime', rels_prt_usetime, 'rels_prt_usetime', '适用季节')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'age', rels_prt_age, 'rels_prt_age', '适用年龄')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'selltime', rels_prt_selltime, 'rels_prt_selltime', '上市时间')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'category', rels_prt_cat3, 'rels_prt_cat3', '所属类别')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'dpgoods', rels_prt_dpgoods, 'rels_prt_dpgoods', '搭配商品')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'scene', rels_prt_usescene, 'rels_prt_usescene', '适用场景')
    # relations.append(rel_set)
    # rel_set = write_rel_json('Product', 'dpcategory', rels_prt_dpcls, 'rels_prt_dpcls', '搭配品类')
    # relations.append(rel_set)
    rel_set = write_rel_json('Product', 'pubgoods', rels_prt_pubgoods, 'rels_prt_pubgoods', '关联商品')
    relations.append(rel_set)
    rel_set = write_rel_json('Product', 'body', rels_prt_body, 'rels_prt_body', '适用部位')
    relations.append(rel_set)
    rel_set = write_rel_json('Product', 'eat', rels_prt_eat, 'rels_prt_eat', '食用方法')
    relations.append(rel_set)
    rel_set = write_rel_json('Product', 'location', rels_prt_loc, 'rels_prt_loc', '适用场地')
    relations.append(rel_set)
    rel_set = write_rel_json('Product', 'bof', rels_prt_bof, 'rels_prt_bof', '触感')
    relations.append(rel_set)
    rel_set = write_rel_json('Product', 'tas', rels_prt_tas, 'rels_prt_tas', '味道')
    relations.append(rel_set)
    filename = './graph_node_data/relations.pick'
    with open(filename, 'wb') as f:
        pickle.dump(relations, f)

    return 'end'


# create_nodes_json()
# create_rels_nodes()
# if __name__ == '__main__':
#     a = product_nodes()

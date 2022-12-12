"""
抽取商品标签
"""
import pickle
import json
import numpy as np
import re

with open('tb_dim_pro_gk_ali_tags_df-1688产品属性表', 'rb') as f:  # used
    tags2goodsfrom1688_df = pickle.load(f)
with open('tb_dim_pro_gk_product_df-产品信息维表', 'rb') as f:  # used
    fromselector2product_df = pickle.load(f)
with open('tb_dim_pro_gk_product_tag_df-产品标签属性表', 'rb') as f:  # used
    res = pickle.load(f)
with open('tb_dwd_pro_product_sale_df-产品商品信息表', 'rb') as f:  # user
    res_ = pickle.load(f)

# todo 存储商品属性信息
tags_giikin = {}

# todo giikin产品信息中包含的标签
for row in res.itertuples():
    product_id = getattr(row, 'product_id')
    tag_name = getattr(row, 'tag_name')
    tag_type = getattr(row, 'tag_type')
    if product_id not in tags_giikin:
        tags_giikin[product_id] = {}
        tags_giikin[product_id][tag_type] = tag_name
    else:
        tags_giikin[product_id][tag_type] = tag_name

# todo 抽取1688属性都有哪些；giikin的商品大多数来自1688
for row in tags2goodsfrom1688_df.itertuples():
    product_id = getattr(row, 'product_id')
    buyer_url = getattr(row, 'buyer_url')
    product_name_1688 = getattr(row, 'product_name')
    category_name_1688 = getattr(row, 'category_name')
    category_id_1688 = getattr(row, 'category_id')
    tags = getattr(row, 'tags')
    tags_list = json.loads(tags)
    if product_id not in tags_giikin:
        tags_giikin[product_id] = {}
        tags_giikin[product_id]['材质标签'] = []
        # tags_giikin[product_id]['buyer_url'] = buyer_url  # giikin 产品维表中也有这个字段，以产品维表为准
        tags_giikin[product_id]['product_name_1688'] = product_name_1688
        # fixme 这里结束后缺失1688类别name信息，只有1688类别id
        tags_giikin[product_id]['category_id_1688'] = category_id_1688
        for tag in tags_list:
            tmpkv = list(tag.values())
            if tmpkv[0] == '材质标签':
                tags_giikin[product_id][tmpkv[0]].append(tmpkv[1])
            tags_giikin[product_id][tmpkv[0]] = tmpkv[1]
    else:
        tags_giikin[product_id]['材质标签'] = []
        # tags_giikin[product_id]['buyer_url'] = buyer_url
        tags_giikin[product_id]['product_name'] = product_name_1688
        tags_giikin[product_id]['category_id_1688'] = category_id_1688
        for tag in tags_list:
            tmpkv = list(tag.values())
            if tmpkv[0] == '材质标签':
                tags_giikin[product_id][tmpkv[0]].append(tmpkv[1])
            tags_giikin[product_id][tmpkv[0]] = tmpkv[1]

    if len(tags_giikin[product_id]['材质标签']) == 0:
        tags_giikin[product_id].pop('材质标签')

# todo giikin data information  - 这个表中的数据包含了大量商品属性信息(上面1688的属性解析完毕，现在解析giikin的属性信息)
is_sensitive_type = {'0': '非敏感', '1': '敏感货', '2': '液体', '3': '易燃易爆品', '4': '动漫IP',
                     '5': '危险药', '6': '防疫品', '7': '特殊货', '8': '带电带磁', '9': '易碎', '10': '食品', }
pack_types = {0: '无需包装', 1: '气泡袋', 2: '纸箱', }
delivery_methods = {1: '京东代发', 2: '供应商代发', 3: '深圳直发', 4: '其他'}
# 去掉包含None值的行    # 删除一些重要字段中，有值为None的行，可能会删除很多数据，因此不暴力删除None值
# fromselector2product_df_ = fromselector2product_df.dropna(axis=0, subset=['is_sensitive', 'volume'])
# df = fromselector2product_df.replace(to_replace='None', value=np.nan).dropna()
# for row in fromselector2product_df_.itertuples():
for row in fromselector2product_df.itertuples():
    product_id = getattr(row, 'product_id')
    if product_id not in tags_giikin:
        tags_giikin[product_id] = {}
    else:
        pass
    store = tags_giikin[product_id]
    # fixme category_id_giikin 的清洗处理，这个字段和下面的category_lvl3_id字段有的是重复的, 故取消这个字段的读取
    # category_id_giikin = getattr(row, 'category_id')
    # if category_id_giikin == float('nan'):
    #     category_id_giikin = -1
    category_lvl3_name_giikin = getattr(row, 'category_lvl3_name')
    category_lvl3_id_giikin = getattr(row, 'category_lvl3_id')
    store['category_lvl3_name_giikin'] = category_lvl3_name_giikin
    store['category_lvl3_id_giikin'] = category_lvl3_id_giikin
    # category_id_1688 = getattr(row, 'category_1688')  # 1688分类id 从ali表中获取
    # store['category_id_1688'] = category_id_1688

    product_name_giikin = getattr(row, 'product_name')
    store['product_name'] = product_name_giikin

    # seller_id 的清洗处理
    seller_id = getattr(row, 'seller_id')  # 主供应商ID
    if np.isnan(seller_id):
        seller_id = -1
    store['主供应商'] = int(seller_id)

    # 采购员编号的清洗处理
    buyer = getattr(row, 'buyer')  # 采购员
    if np.isnan(buyer):
        buyer = -1
    store['采购员ID'] = int(buyer)

    # 选品师编号的清洗处理
    selection = getattr(row, 'selection')  # 选品师
    if np.isnan(selection) or int(selection) == 0:
        selection = -1
    store['选品师'] = int(selection)

    # 意味着到这一步时已经有了的字段
    buyer_url = getattr(row, 'purchase_url')  # 采购地址，已经有了buyer_url字段，和这个purchase_url是一致的字段含义，故不重复添加
    is_gift = getattr(row, 'is_gift')  # 赠品标签
    store['采购地址'] = buyer_url
    if np.isnan(is_gift) is False:
        store['是否赠品'] = int(is_gift)

    # 判断敏感品属性的处理相对复杂
    is_sensitive = getattr(row, 'is_sensitive')
    if is_sensitive is not None and len(is_sensitive) >= 3:
        is_sensitives = [is_sensitive_type[_] for _ in is_sensitive.split(',')]
    elif not is_sensitive or is_sensitive == '0':
        is_sensitives = '非敏感'
    else:
        is_sensitives = is_sensitive_type[is_sensitive]
    store['是否敏感货物'] = is_sensitives

    delivery_place = getattr(row, 'delivery_place')  # 发货地
    store['发货地'] = delivery_place

    # fixme 暂时是以采购价为准
    # product_price = getattr(row, 'product_price')  # 产品的价格/ 产品单价/采购价

    # 产品体积信息的处理相对复杂
    volume = getattr(row, 'volume')
    if volume is not None:
        volume = re.sub(r'[A-Za-z]', '', volume).strip()
        volume = re.sub(r'[\u4e00-\u9fa5]+', '', volume).strip()
        volume = re.sub(r"""[!?'".<>(){}【】@%&/[/]""", '', volume).strip()
        product = float(1)
        try:
            if volume and '*' in volume:
                param = volume.split('*')
                for _ in param:
                    product = product * float(_)
            elif volume and 'X' in volume:
                param = volume.split('X')
                for _ in param:
                    product = product * float(_)
            elif volume and '×' in volume:
                param = volume.split('×')
                for _ in param:
                    product = product * float(_)
            else:
                product = float(volume)
            store['产品体积'] = product
        except:
            # logging.warning(str(volume))
            product = float(-1)
            store['产品体积'] = product

    note = getattr(row, 'note')  # 采购备注
    if note is not None:
        note = re.sub('【采购备注】', '', note).strip()
        note = re.sub('【选品备注】', '', note).strip()
        store['采购备注'] = note

    product_image = getattr(row, 'product_image')  # 产品图片   url
    if product_image:
        store['产品图片'] = product_image
    selling_point = getattr(row, 'selling_point')  # 买点提取
    if selling_point:
        store['卖点提取'] = selling_point
    weight = getattr(row, 'weight')
    if weight:
        store['产品重量'] = weight

    is_bubble_cushion = getattr(row, 'is_bubble_cushion')  # 是否需要气泡垫  0-否 | 1-是
    is_bubble_cushion = '是' if is_bubble_cushion == 1 else '否'
    store['气泡垫'] = is_bubble_cushion

    is_epidemic = getattr(row, 'is_epidemic')  # 是否防疫物品  0-否 | 1-是
    is_epidemic = '是' if is_epidemic == 1 else '否'
    store['防疫物品'] = is_epidemic

    is_jewelry_box = getattr(row, 'is_jewelry_box')  # 饰品盒  0-否 | 1-是
    is_jewelry_box = '是' if is_jewelry_box == 1 else '否'
    store['饰品盒'] = is_jewelry_box
    if is_jewelry_box == '是':
        jewelry_box_spec = getattr(row, 'jewelry_box_spec')  # 饰品盒规格
        store['饰品盒规格'] = jewelry_box_spec
        jewelry_box_product_rate = getattr(row, 'jewelry_box_product_rate')  # 饰品盒产品比例
        store['饰品盒产品比例'] = jewelry_box_product_rate

    # fixme 这些字段已经不用了
    # product_type = getattr(row, 'product_type')
    # store.append({'产品类型': product_type})

    # fixme 签收率以及采购单价是几个重要指标
    purchase_price = getattr(row, 'purchase_price')  # 采购价
    if purchase_price:
        store['采购价'] = purchase_price
    sign_rate = getattr(row, 'sign_rate')  # 签收率
    if sign_rate:
        store['签收率'] = sign_rate

        # fixme 包装类型 pack_types = {0: '无需包装', 1: '气泡袋', 2: '纸箱', } 这里的包装枚举是不全的
    pack_type = getattr(row, 'pack_type')
    tmptype = pack_types.get(pack_type)
    pack_type = tmptype if tmptype else '其他'
    store['包装类型'] = pack_type
    agent_seller = getattr(row, 'agent_seller')  # 代发供应商
    if agent_seller and np.isnan(agent_seller) is False:
        store['代发供应商'] = agent_seller
    # fixme 发货方式  这个字段的枚举也是不全的  delivery_methods = {1: '京东代发', 2: '供应商代发', 3: '深圳直发', 4: '缺失未知'}
    delivery_method = getattr(row, 'delivery_method')
    if np.isnan(delivery_method):
        delivery_method = delivery_methods[4]
    elif delivery_methods.get(delivery_method) is None:
        delivery_method = delivery_methods[4]
    else:
        delivery_method = delivery_methods[delivery_method]
    store['发货方式'] = delivery_method

    eng_name = getattr(row, 'eng_name')
    if eng_name:
        store['eng_name'] = eng_name

    """ 鲁班系统订单的产品审核 0 不需审核 1 未审核 2 审核通过 """
    luban_examines = {0: '不需审核', 1: '未审核', 2: '审核通过'}
    luban_examine = getattr(row, 'luban_examine')
    luban_examine = luban_examines[luban_examine]
    store['鲁班系统订单的产品审核'] = luban_examine
    """ 是否支持退货 1 不支持 2 支持 """
    is_return = getattr(row, 'is_return')
    is_return = '不支持' if is_return == 1 else '支持'
    store['是否支持退货'] = is_return

    # is_product_hot = getattr(row, 'is_product_hot')  # fixme 是否添加到热销产品 | 产品的这个字段都是1开启状态，因此没有任何意义
    # material_name = getattr(row, 'material_name')  # fixme 产品维表中产品材质名称这个字段基本都是null，故使用ali product tags 表
    descript = getattr(row, 'descript')  # fixme 这个字段也基本都是 null
    if descript:
        store['产品描述'] = descript

    # fixme 这几个参数对于商品选择挺重要的，但是都是None：
    #  sunrise_volume（日出货量）/production_cycle（生产周期）/main_push（主推款）
    #  logistics_cost（物流成本）/selling_point（卖点）/from_platform（来源平台）/product_brand（品牌）

# todo giikin产品商品信息 - 去除一些选品师是None的信息
res_ = res_.dropna(axis=0, subset=['chooser_name'])
for row in res_.itertuples():
    product_id = getattr(row, 'product_id')
    if product_id not in tags_giikin:
        tags_giikin[product_id] = {}
    else:
        pass
    store = tags_giikin[product_id]
    product_name_giikin = getattr(row, 'product_name')
    if product_name_giikin and 'product_name' not in tags_giikin:
        tags_giikin['product_name'] = product_name_giikin

    sale_id = getattr(row, 'sale_id')
    if sale_id:
        tags_giikin['商品ID'] = sale_id

    sale_name = getattr(row, 'sale_name')
    if sale_name:
        tags_giikin['优化投放商品name'] = sale_name

    # fixme 这个字段的映射并不清晰
    sale_status = getattr(row, 'sale_status')
    if sale_status:
        tags_giikin['商品状态'] = sale_status

    # fixme 这里之所以这样是因为这里有很多老品新上的问题
    product_create_time = getattr(row, 'product_create_time')
    sale_create_time = getattr(row, 'sale_create_time')
    push_goods_time = product_create_time + '/' + sale_create_time
    tags_giikin['产品投放时间'] = push_goods_time

    tags_giikin['团队名称'] = getattr(row, 'team_name')
    tags_giikin['语种线路'] = getattr(row, 'lang_name')
    tags_giikin['币种'] = getattr(row, 'currency_name')

    tags_giikin['选品师name'] = getattr(row, 'chooser_name')
    opt_name = getattr(row, 'opt_name')
    if opt_name is not None:
        tags_giikin['优化师name'] = opt_name

    buyer_name = getattr(row, 'buyer_name')
    if buyer_name:
        tags_giikin['采购员name'] = buyer_name

    designer_name = getattr(row, 'designer_name')
    if designer_name:
        tags_giikin['设计师name'] = designer_name

    product_price = getattr(row, 'product_price')
    if product_price:
        tags_giikin['产品售价'] = product_price

# important  最后存储的tags文件在这里：./giikin_prtswithstandards_tags.pick

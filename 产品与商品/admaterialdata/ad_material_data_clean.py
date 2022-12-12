# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:ad_material_data_clean.py
@Time:2022/12/9 10:46
@Read: 商品ID和产品ID和清洗后的文本的融合; 汇总数据用这个；
"""
import re
import pickle
from zhconv import convert


def cleantext(i):
    utl_pat_1 = re.compile(r'\<http.+?\>', re.DOTALL)  # 去除URL
    utl_pat_2 = re.compile(r'\<https.+?\>', re.DOTALL)  # 去除URL
    jap = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\uAC00-\uD7A3]')  # 去除日文行
    patt = '^[a-zA-Z0-9’!"$%&\'()*+,./:;<=>?@，。?、…【】《》？“”‘’！[\\]^_`{|}\s]+'  # 去除特殊字符
    pattern = re.compile(u'[^\u4e00-\u9fa5]')  # 匹配非中文
    s = "\n\r\t@#$%^&*这样一本书大卖，有点意外，据说已经印了四五十万，排行榜仅次于《希拉里自传》。推荐倒着看。\n\s\r\t"
    patchnend = '[\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\u4e00-\u9fa5]'  # 匹配中文和中文标点符号
    t = re.findall('[\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\u4e00-\u9fa5]', s)
    # print(''.join(t))

    # prtname = getattr(i, 'product_name')
    # prtname = re.sub(utl_pat_1, '', prtname)
    # prtname = re.sub(utl_pat_2, '', prtname)
    # prtname = re.sub(jap, '', prtname)
    # prtname = re.sub(patt, '', prtname)
    # prtname = re.findall(patchnend, prtname)
    # prtname = ''.join(prtname)

    sale_name = i['sale_name']  # sale_name
    if sale_name is None:
        sale_name = i['product_name']
    if sale_name is None:
        return
    if not isinstance(sale_name, str):
        return
    sale_name = convert(sale_name, 'zh-cn')
    sale_name = re.sub(utl_pat_1, '', sale_name)
    sale_name = re.sub(utl_pat_2, '', sale_name)
    sale_name = re.sub(jap, '', sale_name)
    sale_name = re.sub(patt, '', sale_name)
    sale_name = re.findall(patchnend, sale_name)
    sale_name = ''.join(sale_name)

    ad_slogans = i['ad_slogans']  # ad_slogans
    if ad_slogans is None:
        return
    ad_slogans = convert(ad_slogans, 'zh-cn')
    ad_slogans = re.sub(utl_pat_1, '', ad_slogans)
    ad_slogans = re.sub(utl_pat_2, '', ad_slogans)
    ad_slogans = re.sub(jap, '', ad_slogans)
    ad_slogans = re.sub(patt, '', ad_slogans)
    ad_slogans = re.findall(patchnend, ad_slogans)
    ad_slogans = ''.join(ad_slogans)
    cleantext = sale_name + '。' + ad_slogans
    return cleantext


with open('./readadtextdatabase.pick', 'rb') as f:
    # # todo data clean: DF的apply方法，在指定axis=1的情况下，已经完成了内部迭代，
    matgoodsinfo = pickle.load(f)
    # res = matgoodsinfo.head(100)
    # res.loc[:, 'cleantext'] = res.apply(cleantext, axis=1)  # todo 这样子做会把整个DF都遍历一遍后再返回结果
    matgoodsinfo.loc[:, 'cleantext'] = matgoodsinfo.apply(cleantext, axis=1)  # todo 这样子做会把整个DF都遍历一遍后再返回结果

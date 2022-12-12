# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:ad_material_data_clean_2ner.py
@Time:2022/12/9 10:53
@Read: 生成可以用来做NER标注的数据
"""

import re
import pickle
import random
import logging
from zhconv import convert

random.seed(2022)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s', datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

s = "\n\r\t@#$%^&*这样一本书大卖，有点意外，据说已经印了四五十万，排行榜仅次于《希拉里自传》。推荐倒着看。\n\s\r\t"
patternnochn = re.compile(u'[^\u4e00-\u9fa5]')  # 匹配非中文
utl_pat_1 = re.compile(r'\<http.+?\>', re.DOTALL)  # 去除URL
utl_pat_2 = re.compile(r'\<https.+?\>', re.DOTALL)  # 去除URL
jap = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\uAC00-\uD7A3]')  # 去除日文行
patt = '^[a-zA-Z0-9’!"$%&\'()*+,./:;<=>?@，。?、…【】《》？“”‘’！[\\]^_`{|}\s]+'  # 去除特殊字符
patchnend = '[\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\u4e00-\u9fa5]'  # 匹配中文和中文标点符号
t = re.findall('[\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\u4e00-\u9fa5]',
               s)  # print(''.join(t))

# # todo clean data for NER
clean_text = []
# 读取的这部分数据是material表的原始数据
with open('./readadtextdatabase.pick', 'rb') as f:
    mate_data = pickle.load(f)
    for i in mate_data.itertuples():
        # prtname = getattr(i, 'product_name')
        # prtname = re.sub(utl_pat_1, '', prtname)
        # prtname = re.sub(utl_pat_2, '', prtname)
        # prtname = re.sub(jap, '', prtname)
        # prtname = re.sub(patt, '', prtname)
        # prtname = re.findall(patchnend, prtname)
        # prtname = ''.join(prtname)

        sale_name = getattr(i, 'sale_name')
        if sale_name is None:
            sale_name = getattr(i, 'product_name')
        if sale_name is None:
            continue
        sale_name = convert(sale_name, 'zh-cn')
        sale_name = re.sub(utl_pat_1, '', sale_name)
        sale_name = re.sub(utl_pat_2, '', sale_name)
        sale_name = re.sub(jap, '', sale_name)
        sale_name = re.sub(patt, '', sale_name)
        sale_name = re.findall(patchnend, sale_name)
        sale_name = ''.join(sale_name)

        ad_slogans = getattr(i, 'ad_slogans')
        if ad_slogans is None:
            continue
        ad_slogans = convert(ad_slogans, 'zh-cn')
        ad_slogans = re.sub(utl_pat_1, '', ad_slogans)
        ad_slogans = re.sub(utl_pat_2, '', ad_slogans)
        ad_slogans = re.sub(jap, '', ad_slogans)
        ad_slogans = re.sub(patt, '', ad_slogans)
        ad_slogans = re.findall(patchnend, ad_slogans)
        ad_slogans = ''.join(ad_slogans)

        clean_text.append(sale_name + '。' + ad_slogans)

with open('ad_material_data_clean_2ner_clean_end.pick', 'wb') as f:
    pickle.dump(clean_text, f)



# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author: UmeAI
@Blog(个人博客地址): https://www.umeai.top
@File:read_clean2ner_data.py
@Time:2022/12/12 23:41
@todo: 读取只清洗了NER类型的广告素材数据以划分Test数据集
@fixme: 
@important: 
"""
import pickle
import json
import random

with open('../admaterialdata/ad_material_data_clean_2ner_clean_end.pick', 'rb') as f:
    clean_text = pickle.load(f)

random.shuffle(clean_text)

testdata = [{'id': idx, 'text': i} for idx, i in enumerate(clean_text[:50])]
testdata = json.dumps(testdata, ensure_ascii=False)
with open('test.json', 'w', encoding='utf-8') as f:
    f.write(testdata)

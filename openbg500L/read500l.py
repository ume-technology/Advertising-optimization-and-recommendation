# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:read500l.py
@Time:2022/12/9 0:27
@Read: 
"""
# 获取主schema数据 OpenBG-500L
with open('./OpenBG500-L_train.tsv', 'r', encoding='utf-8') as fp:
    # train_data = fp.readlines()
    train_ori_500L = [line.strip('\n').split('\t') for line in fp.readlines()]
    # _ = [print(line) for line in train_ori_500L[:2]]

    # ['ent_135492', 'rel_0352', 'ent_015651']
    # ['ent_020765', 'rel_0448', 'ent_214183']

with open('./OpenBG500-L_entity2text.tsv', 'r', encoding='utf-8') as fp:
    # entity_data = fp.readlines()
    entity_lines_500L = [line.strip('\n').split('\t') for line in fp.readlines()]
    # _ = [print(line) for line in entity_lines_500L[:2]]

    # ['ent_101705', '短袖T恤']
    # ['ent_116070', '套装']

with open('./OpenBG500-L_relation2text.tsv', 'r', encoding='utf-8') as fp:
    # relation_data = fp.readlines()
    relation_lines_500L = [line.strip().split('\t') for line in fp.readlines()]
    # _ = [print(line) for line in relation_lines_500L[:2]]

    # ['rel_0418', '细分市场']
    # ['rel_0290', '关联场景']

ent2text_500L = {line[0]: line[1] for line in entity_lines_500L}
del entity_lines_500L
rel2text_500L = {line[0]: line[1] for line in relation_lines_500L}
del relation_lines_500L
train_standard_500L = [[ent2text_500L[line[0]], rel2text_500L[line[1]], ent2text_500L[line[2]]]
                       for line in train_ori_500L]
del train_ori_500L
# _ = [print('500L：', line) for line in train_standard_500L[:2]]
# ['苦荞茶', '外部材质', '苦荞麦']
# ['精品三姐妹硬糕', '口味', '原味硬糕850克【10包40块糕】']

all_needs_500L = dict()
for i in train_standard_500L:
    if i[1] not in all_needs_500L:
        all_needs_500L[i[1]] = []
    else:
        all_needs_500L[i[1]].append(i)
del train_standard_500L

# todo Concept  细分市场
all_needs_inmarket = set()
# todo Concept 适用时间
all_needs_usetime = set()
# todo Concept 人群
all_needs_groups = set()
# todo Concept 场景
all_needs_scene = set()
# todo Concept 主题
all_needs_theme = set()
for k, v in all_needs_500L.items():
    if k == '细分市场':
        for i in v:
            all_needs_inmarket.add(i[2])
    if k == '适用时间':
        for i in v:
            all_needs_usetime.add(i[2])
    if '适用人群' in k:
        for i in v:
            all_needs_groups.add(i[2])
    if k == '关联场景':
        for i in v:
            all_needs_scene.add(i[2])
    if '主题' in k:
        for i in v:
            all_needs_theme.add(i[2])

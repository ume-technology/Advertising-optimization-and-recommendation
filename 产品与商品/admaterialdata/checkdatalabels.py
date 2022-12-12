# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@Blog: https://www.umeai.top/
@File: checkdatalabels.py
@Time: 2022/10/21 15:36
@ReadMe: 如果完成了Train data的数据标注，在这里检测标注的标签是否有错误信息
"""
with open(
        '../../../Downloads/Advertising-optimization-and-recommendation/giikindataset/ad_material_data/tagdata/giikinnerdata.txt', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    labset = set()
    try:
        for idx, line in enumerate(lines):
            if line == '\n':
                continue
            lab = line.split()[-1]
            if lab == '，O':
                print(idx)
                print(line)
            labset.add(lab)
    except:
        print(idx)
        print(line)

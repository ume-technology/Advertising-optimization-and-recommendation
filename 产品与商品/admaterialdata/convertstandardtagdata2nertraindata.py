# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:convertstandardtagdata2nertraindata.py
@Time:2022/12/9 11:27
@Read: 将check过的标准的BIES标注数据转化为NER数据
"""

import json
import os
import re
import json


def getSpanLabelCLS():
    # todo 一共14种LABEL CLS；写入CRF label = 14*4+1（O）  读取SPAN LABEL CLS; 这里生成的这个文件是TC需要的标准文件；可以准确的生成标签的类型
    labelcls = set()
    with open('./newdata2ner/alldata.txt', 'r', encoding='utf-8') as f:  # 新做出的BIES标注且check后的数据，准备做TCNER数据的格式转换
        # with open('alldata.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for i in lines:
            i = i.strip()
            if not i:
                continue
            if i.split()[1] == 'O':
                continue
            tag = i.split()[1].strip()
            labelcls.add(tag.split('-')[1])

    # todo 写入span ent2id 和 crf ent2id
    SPAN_ENT2ID = {}
    for idx, k in enumerate(labelcls):
        if k == 'O':
            continue
        SPAN_ENT2ID[k] = idx + 1

    # important 这里生成的 CRF ent2id和SPAN ent2id和TC的标准格式是一致的; 且这里的标签必须要全；如果已经完全生成，可以不再进行这个数据的写入；
    span_ent2id = json.dumps(SPAN_ENT2ID)
    with open('../mid_data/span_ent2id.json', 'w') as f:
        f.write(span_ent2id)
    ent_types = ['O'] + [p + '-' + _type for p in ['B', 'I', 'E', 'S'] for _type in list(SPAN_ENT2ID)]
    CRF_ENT2ID = {ent: i for i, ent in enumerate(ent_types)}
    crf_ent2id = json.dumps(CRF_ENT2ID)
    with open('../mid_data/crf_ent2id.json', 'w') as f:
        f.write(crf_ent2id)


def preprocess(input_path, save_path, mode):
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    data_path = os.path.join(save_path, mode + ".json")  # ./mid_data/train.json
    labels = set()
    result = []
    tmp = {}
    tmp['id'] = 0
    tmp['text'] = ''
    tmp['labels'] = []
    # =======先找出句子和句子中的所有实体和类型=======
    with open(input_path, 'r', encoding='utf-8') as fp:
        lines = fp.readlines()
        texts = []
        entities = []
        words = []
        entity_tmp = []
        entities_tmp = []
        for line in lines:
            line = line.strip().split(" ")
            if len(line) == 2:
                word = line[0]
                label = line[1]
                words.append(word)
                if "B-" in label:
                    entity_tmp.append(word)
                elif "I-" in label:
                    entity_tmp.append(word)
                elif "E-" in label:
                    entity_tmp.append(word)
                    if ("".join(entity_tmp), label.split("-")[-1]) not in entities_tmp:
                        entities_tmp.append(("".join(entity_tmp), label.split("-")[-1]))
                    labels.add(label.split("-")[-1])
                    entity_tmp = []
                if "S-" in label:
                    entity_tmp.append(word)
                    if ("".join(entity_tmp), label.split("-")[-1]) not in entities_tmp:
                        entities_tmp.append(("".join(entity_tmp), label.split("-")[-1]))
                    entity_tmp = []
                    labels.add(label.split("-")[-1])
            else:
                texts.append("".join(words))
                entities.append(entities_tmp)
                words = []
                entities_tmp = []
        # for text, entity in zip(texts, entities):
        #     print(text, entity)
        # print(labels)
    # =======找出句子中实体的位置=======
    i = 0
    for text, entity in zip(texts, entities):
        if entity:
            ltmp = []
            for ent, type in entity:
                for span in re.finditer(ent, text):
                    start = span.start()
                    end = span.end()
                    ltmp.append((type, start, end, ent))
            ltmp = sorted(ltmp, key=lambda x: (x[1], x[2]))
            tmp['id'] = i
            tmp['text'] = text
            for j in range(len(ltmp)):
                tmp['labels'].append(["T{}".format(str(j)), ltmp[j][0], ltmp[j][1], ltmp[j][2], ltmp[j][3]])
        else:
            tmp['id'] = i
            tmp['text'] = text
            tmp['labels'] = []
        result.append(tmp)
        # print(i, text, entity, tmp)
        tmp = {}
        tmp['id'] = 0
        tmp['text'] = ''
        tmp['labels'] = []
        i += 1

    # todo 写入   important  这里存储的是Train data数据，但是按照TC的思路，这里本质上是一个stack数据，只需要手动copy train to stack.json即可；
    #                       生成该文件之后需要执行split脚本把这个stack数据分割为 train 和 dev 数据集;
    with open(data_path, 'w', encoding='utf-8') as fp:  # # ./mid_data/train.json
        fp.write(json.dumps(result, ensure_ascii=False))

    # todo: 生成本次NER任务一共有多少中标签，这个标签数据可以不写；
    #       例如本次giikin任务的标签：["SCE", "LOC", "OBJ", "BOD", "TAS", "SPO", "PRT", "FUNC", "AGE", "TIME", "MAT", "CON", "BOF", "EAT"]
    if mode == "train":
        label_path = os.path.join(save_path, "labels.json")
        # with open(label_path, 'w', encoding='utf-8') as fp:
        #     fp.write(json.dumps(list(labels), ensure_ascii=False))


# important  这里存储的是Train data数据，但是按照TC的思路，这里本质上是一个stack数据，只需要手动copy train to stack.json即可；
preprocess("./newdata2ner/alldata.txt", '../mid_data', "train")  # 本质上是tc要求的 stack 文件；
# important 生成该文件之后需要执行split脚本把这个stack数据分割为 train 和 dev 数据集; 因此dev和test数据不在这里生成；
# preprocess("dev.char.bmes", '../mid_data', "dev")
# preprocess("test.char.bmes", '../mid_data', "test")


# todo 这里生成的就是CRF ent2id数据文件；已经在getSpanLabelCLS()都生成，因此这里可以不再执行
# labels_path = os.path.join('../mid_data/labels.json')
# with open(labels_path, 'r') as fp:
#     labels = json.load(fp)
# tmp_labels = []
# tmp_labels.append('O')
# for label in labels:
#     tmp_labels.append('B-' + label)
#     tmp_labels.append('I-' + label)
#     tmp_labels.append('E-' + label)
#     tmp_labels.append('S-' + label)
# label2id = {}
# for k, v in enumerate(tmp_labels):
#     label2id[v] = k
# path = '../mid_data'
# if not os.path.exists(path):
#     os.makedirs(path)
# else:
#     # with open(os.path.join(path, "ner_ent2id.json"), 'w') as fp:
#     #     fp.write(json.dumps(label2id, ensure_ascii=False))
#     pass

# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:splittraindata.py
@Time:2022/11/1 13:38
@Read: 已经在convert script中生成了train.txt(stack)数据，这个脚本是是把stack数据分割成train和dev文件；
"""
import json
from sklearn.model_selection import train_test_split, KFold

# todo 执行这个脚本之前，先把train.txt copy出来一份为stack.txt到raw_data文件夹下，总之train.txt和stack.txt文件是一致的，更新了train，stack也要同步更新
with open('./stack.json', 'r', encoding='utf-8') as f:
    stack_examples = json.load(f)
    for i in stack_examples:
        i['pseudo'] = 0
    # todo 构建实体知识库
    kf = KFold(10)
    entities = set()  # 存储的是训练集中的所有的标签数据
    ent_types = set()  # 存储有哪些实体类型
    for _now_id, _candidate_id in kf.split(stack_examples):  # 十折交叉取数：前100 + 后900
        now = [stack_examples[_id] for _id in _now_id]  # 对于第一折来讲： 后900；其他折以此类推
        candidate = [stack_examples[_id] for _id in _candidate_id]  # 对于第一折来讲： 前100；其他折以此类推
        now_entities = set()
        for _ex in now:  # now_entities：对于第一折而言：后900折数据中的实体信息
            for _label in _ex['labels']:
                ent_types.add(_label[1])
                if len(_label[-1]) > 1:
                    now_entities.add(_label[-1])
                    entities.add(_label[-1])
        for _ex in candidate:  # 把后900数据中的实体信息往前100折的数据中做匹配； candidate 是前100的数据；
            text = _ex['text']
            candidate_entities = []
            for _ent in now_entities:  # 后900数据中的实体信息
                if _ent in text:
                    candidate_entities.append(_ent)
            _ex['candidate_entities'] = candidate_entities

    train, dev = train_test_split(stack_examples, shuffle=True, random_state=123, test_size=0.1)

    with open('./train.json', 'w', encoding='utf-8') as fp:
        fp.write(json.dumps(train, ensure_ascii=False))
    with open('./dev.json', 'w', encoding='utf-8') as fp:
        fp.write(json.dumps(dev, ensure_ascii=False))
    pass

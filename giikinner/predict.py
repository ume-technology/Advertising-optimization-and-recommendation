# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:predict.py
@Time:2022/12/9 12:27
@Read: 
"""

import pandas as pd
import pickle
import os
import json
import torch
import math
from collections import defaultdict
from transformers import BertTokenizer
from giikinner.src.model_utils import CRFModel, SpanModel, EnsembleCRFModel, EnsembleSpanModel
from giikinner.src.evaluator import crf_decode, span_decode
from giikinner.src.functions_utils import load_model_and_parallel, ensemble_vote
from giikinner.src.processor import cut_sent, fine_grade_tokenize

MID_DATA_DIR = "datagiikin/mid_data"
RAW_DATA_DIR = "datagiikin/raw_data"
SUBMIT_DIR = "./result"
GPU_IDS = "-1"
LAMBDA = 0.3
THRESHOLD = 0.9
MAX_SEQ_LEN = 512
TASK_TYPE = "crf"
VOTE = False
BERT_DIR = r'F:\allinoneisfast\premodelfiles\hflchinese-roberta-wwm-ext'
CKPT_PATH = r'F:\allinoneisfast\giikinkgpart-ner\outgiikinner\roberta_wwm_crf\checkpoint-533\model.pt'


def base_predict(model, device, info_dict, ensemble=False, mixed=''):
    labels = defaultdict(list)
    tokenizer = info_dict['tokenizer']
    id2ent = info_dict['id2ent']
    with torch.no_grad():
        for _ex in info_dict['examples']:
            ex_idx = _ex['id']
            raw_text = _ex['text']
            if raw_text is None:
                continue
            if not len(raw_text):
                labels[ex_idx] = []
                print('{}为空'.format(ex_idx))
                continue
            sentences = cut_sent(raw_text, MAX_SEQ_LEN)
            start_index = 0
            for sent in sentences:
                sent_tokens = fine_grade_tokenize(sent, tokenizer)
                encode_dict = tokenizer.encode_plus(
                    text=sent_tokens, max_length=MAX_SEQ_LEN, is_pretokenized=True, pad_to_max_length=False,
                    return_tensors='pt', return_token_type_ids=True, return_attention_mask=True
                )
                model_inputs = {'token_ids': encode_dict['input_ids'], 'attention_masks': encode_dict['attention_mask'], 'token_type_ids': encode_dict['token_type_ids']}
                for key in model_inputs:
                    model_inputs[key] = model_inputs[key].to(device)
                if ensemble:
                    if TASK_TYPE == 'crf':
                        if VOTE:
                            decode_entities = model.vote_entities(model_inputs, sent, id2ent, THRESHOLD)
                        else:
                            pred_tokens = model.predict(model_inputs)[0]
                            decode_entities = crf_decode(pred_tokens, sent, id2ent)
                    else:
                        if VOTE:
                            decode_entities = model.vote_entities(model_inputs, sent, id2ent, THRESHOLD)
                        else:
                            start_logits, end_logits = model.predict(model_inputs)
                            start_logits = start_logits[0].cpu().numpy()[1:1 + len(sent)]
                            end_logits = end_logits[0].cpu().numpy()[1:1 + len(sent)]
                            decode_entities = span_decode(start_logits, end_logits, sent, id2ent)
                else:
                    if mixed:
                        if mixed == 'crf':
                            pred_tokens = model(**model_inputs)[0][0]
                            decode_entities = crf_decode(pred_tokens, sent, id2ent)
                        else:
                            start_logits, end_logits = model(**model_inputs)
                            start_logits = start_logits[0].cpu().numpy()[1:1 + len(sent)]
                            end_logits = end_logits[0].cpu().numpy()[1:1 + len(sent)]
                            decode_entities = span_decode(start_logits, end_logits, sent, id2ent)
                    else:
                        if TASK_TYPE == 'crf':
                            pred_tokens = model(**model_inputs)[0][0]
                            decode_entities = crf_decode(pred_tokens, sent, id2ent)
                        else:
                            start_logits, end_logits = model(**model_inputs)
                            start_logits = start_logits[0].cpu().numpy()[1:1 + len(sent)]
                            end_logits = end_logits[0].cpu().numpy()[1:1 + len(sent)]
                            decode_entities = span_decode(start_logits, end_logits, sent, id2ent)
                for _ent_type in decode_entities:
                    for _ent in decode_entities[_ent_type]:
                        tmp_start = _ent[1] + start_index
                        tmp_end = tmp_start + len(_ent[0])
                        assert raw_text[tmp_start: tmp_end] == _ent[0]
                        labels[ex_idx].append((_ent_type, tmp_start, tmp_end, _ent[0]))
                start_index += len(sent)
                if not len(labels[ex_idx]):
                    labels[ex_idx] = []
    return labels[ex_idx]


pass
with open(os.path.join(MID_DATA_DIR, f'{TASK_TYPE}_ent2id.json'), encoding='utf-8') as f:
    ent2id = json.load(f)
id2ent = {ent2id[key]: key for key in ent2id.keys()}

tokenizer = BertTokenizer(os.path.join(BERT_DIR, 'vocab.txt'))

model = CRFModel(bert_dir=BERT_DIR, num_tags=len(id2ent))
model, device = load_model_and_parallel(model, GPU_IDS, CKPT_PATH)
model.eval()
pass


def predict(goodsdata):
    info_dict = {}
    sale_id = goodsdata['sale_id']
    cleantext = goodsdata['cleantext']
    if math.isnan(sale_id):
        sale_id = 0
    examples = [{'id': int(sale_id), 'text': cleantext}]
    info_dict['id2ent'] = id2ent
    info_dict['tokenizer'] = tokenizer
    info_dict['examples'] = examples

    # model = CRFModel(bert_dir=BERT_DIR, num_tags=len(info_dict['id2ent']))
    # model, device = load_model_and_parallel(model, GPU_IDS, CKPT_PATH)
    # model.eval()

    labels = base_predict(model, device, info_dict)
    eachtextlabels = {}
    for i in labels:
        if i[0] == 'BOD':
            if '适用部位' not in eachtextlabels:
                eachtextlabels['适用部位'] = set()
            else:
                eachtextlabels['适用部位'].add(i[3])
        if i[0] == 'TIME':
            if '适用时间' not in eachtextlabels:
                eachtextlabels['适用时间'] = set()
            else:
                eachtextlabels['适用时间'].add(i[3])
        if i[0] == 'EAT':
            if '食用方法' not in eachtextlabels:
                eachtextlabels['食用方法'] = set()
            else:
                eachtextlabels['食用方法'].add(i[3])
        if i[0] == 'LOC':
            if '适用地点与场景' not in eachtextlabels:
                eachtextlabels['适用地点与场景'] = set()
            else:
                eachtextlabels['适用地点与场景'].add(i[3])
        if i[0] == 'OBJ':
            if '商品适用对象' not in eachtextlabels:
                eachtextlabels['商品适用对象'] = set()
            else:
                eachtextlabels['商品适用对象'].add(i[3])
        if i[0] == 'MAT':
            if '材质材料' not in eachtextlabels:
                eachtextlabels['材质材料'] = set()
            else:
                eachtextlabels['材质材料'].add(i[3])
        if i[0] == 'FUNC':
            if '功能功效' not in eachtextlabels:
                eachtextlabels['功能功效'] = set()
            else:
                eachtextlabels['功能功效'].add(i[3])
        if i[0] == 'BOF':
            if '触感' not in eachtextlabels:
                eachtextlabels['触感'] = set()
            else:
                eachtextlabels['触感'].add(i[3])
        if i[0] == 'PRT':
            if '归一化产品' not in eachtextlabels:
                eachtextlabels['归一化产品'] = set()
            else:
                eachtextlabels['归一化产品'].add(i[3])
        if i[0] == 'AGE':
            if '适配年龄' not in eachtextlabels:
                eachtextlabels['适配年龄'] = set()
            else:
                eachtextlabels['适配年龄'].add(i[3])
        if i[0] == 'CON':
            if '商品产地【虚假】' not in eachtextlabels:
                eachtextlabels['商品产地【虚假】'] = set()
            else:
                eachtextlabels['商品产地【虚假】'].add(i[3])
        if i[0] == 'SCE':
            if '适用场景' not in eachtextlabels:
                eachtextlabels['适用场景'] = set()
            else:
                eachtextlabels['适用场景'].add(i[3])
        if i[0] == 'SPO':
            if '特色卖点' not in eachtextlabels:
                eachtextlabels['特色卖点'] = set()
            else:
                eachtextlabels['特色卖点'].add(i[3])
        if i[0] == 'TAS':
            if '口味' not in eachtextlabels:
                eachtextlabels['口味'] = set()
            else:
                eachtextlabels['口味'].add(i[3])
    return eachtextlabels
    # for key in labels.keys():
    #     with open(os.path.join(save_dir, f'{key}.ann'), 'w', encoding='utf-8') as f:
    #         if not len(labels[key]):
    #             print(key)
    #             f.write("")
    #         else:
    #             for idx, _label in enumerate(labels[key]):
    #                 f.write(f'T{idx + 1}\t{_label[0]} {_label[1]} {_label[2]}\t{_label[3]}\n')
    #
    # # with open(os.path.join(RAW_DATA_DIR, 'test.json'), encoding='utf-8') as f:
    # #     info_dict['examples'] = json.load(f)[:100]
    #
    # return info_dict


with open(r'F:\allinoneisfast\giikinkgpart-alicocokb\openkgfulldata\matgoodsinfo_goodsfeatures', 'rb') as f:
    goodsdata = pickle.load(f)
    # goodsdata = goodsdata.head(50)

    # groups = goodsdata.groupby(goodsdata.platfrom).groups
    # plats = ['google', 'facebook', 'line', 'tiktok']

    team_names = goodsdata.groupby(goodsdata.team_name).groups
    teams = [i for i in team_names]

    # designers_list = goodsdata.groupby(goodsdata.designer_id).groups
    # designers = [i for i in designers_list]
    # facebook = goodsdata.loc[goodsdata['platfrom'] == 'facebook']
    # google = goodsdata.loc[goodsdata['platfrom'] == 'google']
    # line = goodsdata.loc[goodsdata['platfrom'] == 'line']
    # tiktok = goodsdata.loc[goodsdata['platfrom'] == 'tiktok']
    # goodsdatalist = {'facebook': facebook, 'google': google, 'line': line, 'tiktok': tiktok}
    # for k, v in goodsdatalist.items():
    #     with open(k, 'wb') as f:
    #         pickle.dump(v, f)

for i in teams:
    _ = goodsdata.loc[goodsdata['team_name'] == i]
    _.loc[:, 'goodsfeatures'] = _.apply(predict, axis=1)
    # with open('./nertags/{}'.format(i), 'wb') as f:
    #     pickle.dump(_, f)

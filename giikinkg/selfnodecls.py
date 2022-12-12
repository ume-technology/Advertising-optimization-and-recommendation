# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:selfnodecls.py
@Time:2022/12/6 9:05
@Read: 自定义数据结构以导入neo4j
"""
import json
from decimal import Decimal

res = str(Decimal('3.40').quantize(Decimal('0.0')))

from py2neo import Graph, Node


class MyDict:
    def __init__(self, l=None, name=None):
        self.dict = l
        self.name = name

    @staticmethod
    def dict_to_object(d):
        return MyDict(d.get('dict'), d.get('name'))

    def to_json(self) -> str:
        return json.dumps(self, default=lambda obj: obj.__dict__, ensure_ascii=False, sort_keys=True)

# ml = MyDict({1: 2, 3: 4}, "1234")
# ml = ml.to_json()
# graph = Graph('http://localhost:7474/', auth=("neo4j", "aa1230.aa"))
# test_node_1 = Node(label="List", value=ml)
# graph.create(test_node_1)

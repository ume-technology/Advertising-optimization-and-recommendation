# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:UmeAI
@File:build_graph.py
@Time:2022/12/5 17:03
@Read:
"""
import os
import json
import numpy as np
import pandas as pd
from decimal import Decimal
from py2neo import Graph, Node
from giikinkg.nodes_product import product_nodes as read_nodes
from giikinkg.selfnodecls import MyDict
import logging

logging.basicConfig(filename='./wrongdatatocreatenode.log', level=logging.INFO)


class MedicalGraph:
    def __init__(self):
        # cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        # self.data_path = os.path.join(cur_dir, '../data/medical.json')
        # self.data_path = '../data/medical.json'

        self.g = Graph('http://localhost:7474/', auth=("neo4j", "aa1230.aa"))
        # self.g = Graph(
        #     host="127.0.0.1",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
        #     http_port=7474,  # neo4j 服务器监听的端口号
        #     user="neo4j",  # 数据库user name，如果没有更改过，应该是neo4j
        #     password="123456")

    def create_node(self, label, nodes):
        """ 建立节点
        :param label:
        :param nodes:
        :return:
        """
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.g.create(node)
            count += 1
            print(count, len(nodes))
        return

    def create_product_infos_nodes(self, product_infos):
        """ 创建知识图谱中心产品的节点
        :param disease_infos:
        :return:
        """
        count = 0
        for idx, prt_dict in enumerate(product_infos):
            node = ''
            input_dict = {}
            for k, v in prt_dict.items():
                if k is None:
                    continue
                if isinstance(v, Decimal):
                    v = str(v.quantize(Decimal('0.0')))
                    input_dict[k] = float(v)
                    continue
                if isinstance(v, Decimal):
                    continue
                if isinstance(v, list):
                    new_v = []
                    for _ in v:
                        if isinstance(_, dict):
                            new_v.append(str(_))
                    input_dict[k] = new_v
                    continue
                if v is None:
                    continue
                if not v:
                    continue
                if isinstance(v, list) and len(v) == 0:
                    continue
                else:
                    input_dict[k] = v

            # tarnode = MyDict(input_dict, 'now_node')
            node = Node("Product", **input_dict)
            try:
                self.g.create(node)
            except Exception as e:
                logging.info('{}\n, create: {}==={}}'.format(e, idx, input_dict))
                # print('Create Node 时出现错误：', {idx: input_dict})
            count += 1
        return

    def create_graphnodes(self):
        """ 创建知识图谱实体节点类型schema
        :return:
        """
        pro_names, coloryuansu_tags, size_tags, group_tags, material_tags, design_tags, function_tags, fengge_tags, sellpoint_tags, usetime_tags, \
        age_tags, selltime_tags, cat3_tags, dpgoods_tags, usescene_tags, dpcls_tags, body_tags, eat_tags, loc_tags, bof_tags, tas_tags, pubgoods_tags, \
        prt_infos, rels_prt_coloryuansu, rels_prt_size, rels_prt_groups, rels_prt_material, rels_prt_functions, rels_prt_design, rels_prt_fengge, rels_prt_sellpoint, \
        rels_prt_usetime, rels_prt_age, rels_prt_selltime, rels_prt_cat3, rels_prt_dpgoods, rels_prt_usescene, rels_prt_dpcls, rels_prt_pubgoods, rels_prt_body, rels_prt_eat, \
        rels_prt_loc, rels_prt_bof, rels_prt_tas = read_nodes()
        self.create_product_infos_nodes(prt_infos)
        self.create_node('Coloryuansu', coloryuansu_tags)
        print(len(coloryuansu_tags))
        self.create_node('Size', size_tags)
        print(len(size_tags))
        self.create_node('Group', group_tags)
        print(len(group_tags))
        self.create_node('Material', material_tags)
        print(len(material_tags))
        self.create_node('Design', design_tags)
        print(len(design_tags))
        self.create_node('Function', function_tags)
        print(len(function_tags))
        self.create_node('Fengge', fengge_tags)
        print(len(fengge_tags))
        self.create_node('SellPoint', sellpoint_tags)
        print(len(sellpoint_tags))
        self.create_node('UseTime', usetime_tags)
        print(len(usetime_tags))
        self.create_node('Age', age_tags)
        print(len(age_tags))
        self.create_node('SellTime', selltime_tags)
        print(len(selltime_tags))
        self.create_node('Cat3', cat3_tags)
        print(len(cat3_tags))
        self.create_node('DPgoogds', dpgoods_tags)
        print(len(dpgoods_tags))
        self.create_node('UseScene', usescene_tags)
        print(len(usescene_tags))
        self.create_node('DPCls', dpcls_tags)
        print(len(dpcls_tags))
        self.create_node('Body', body_tags)
        print(len(body_tags))
        self.create_node('Eat', eat_tags)
        print(len(eat_tags))
        self.create_node('Loc', loc_tags)
        print(len(loc_tags))
        self.create_node('Bof', bof_tags)
        print(len(bof_tags))
        self.create_node('Tas', tas_tags)
        print(len(tas_tags))
        self.create_node('PubGoods', pubgoods_tags)
        print(len(pubgoods_tags))
        return

    def create_graphrels(self):
        """ 创建实体关系边
        :return:
        """
        pro_names, coloryuansu_tags, size_tags, group_tags, material_tags, design_tags, function_tags, fengge_tags, sellpoint_tags, usetime_tags, \
        age_tags, selltime_tags, cat3_tags, dpgoods_tags, usescene_tags, dpcls_tags, body_tags, eat_tags, loc_tags, bof_tags, tas_tags, pubgoods_tags, \
        prt_infos, rels_prt_coloryuansu, rels_prt_size, rels_prt_groups, rels_prt_material, rels_prt_functions, rels_prt_design, rels_prt_fengge, rels_prt_sellpoint, \
        rels_prt_usetime, rels_prt_age, rels_prt_selltime, rels_prt_cat3, rels_prt_dpgoods, rels_prt_usescene, rels_prt_dpcls, rels_prt_pubgoods, rels_prt_body, rels_prt_eat, \
        rels_prt_loc, rels_prt_bof, rels_prt_tas = read_nodes()
        self.create_relationship('Product', 'CorlorYuansu', rels_prt_coloryuansu, 'CorlorYuansu', '流行元素【颜色】')
        self.create_relationship('Product', 'Size', rels_prt_size, 'rels_prt_size', '规则尺寸')
        self.create_relationship('Disease', 'Groups', rels_prt_groups, 'rels_prt_groups', '适用群体')
        self.create_relationship('Department', 'Material', rels_prt_material, 'rels_prt_material', '材质材料')
        self.create_relationship('Disease', 'Function', rels_prt_functions, 'rels_prt_functions', '功能功效')
        self.create_relationship('Producer', 'Design', rels_prt_design, 'rels_prt_design', '款式设计')
        self.create_relationship('Disease', 'Fengge', rels_prt_fengge, 'rels_prt_fengge', '流行元素【风格】')
        self.create_relationship('Disease', 'SellPoint', rels_prt_sellpoint, 'rels_prt_sellpoint', '流行元素【卖点】')
        self.create_relationship('Disease', 'UseTime', rels_prt_usetime, 'rels_prt_usetime', '使用季节')
        self.create_relationship('Disease', 'Age', rels_prt_age, 'rels_prt_age', '使用年龄')
        self.create_relationship('Disease', 'Cat3', rels_prt_cat3, 'rels_prt_cat3', '所属分类')
        self.create_relationship('Disease', 'DPGoods', rels_prt_dpgoods, 'rels_prt_dpgoods', '搭配商品')
        self.create_relationship('Disease', 'Scene', rels_prt_usescene, 'rels_prt_usescene', '适用场景')
        self.create_relationship('Disease', 'DPCls', rels_prt_dpcls, 'rels_prt_dpcls', '搭配品类')
        self.create_relationship('Disease', 'PubGoods', rels_prt_pubgoods, 'rels_prt_pubgoods', '投放商品')
        self.create_relationship('Disease', 'Body', rels_prt_body, 'rels_prt_body', '作用部位')
        self.create_relationship('Disease', 'Eat', rels_prt_pubgoods, 'rels_prt_eat', '食用方法')
        self.create_relationship('Disease', 'Loc', rels_prt_loc, 'rels_prt_loc', '适用场合')
        self.create_relationship('Disease', 'Bof', rels_prt_bof, 'rels_prt_bof', '质感触感')
        self.create_relationship('Disease', 'Tas', rels_prt_tas, 'rels_prt_tas', '口味')

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        """ 创建实体关联边
        :param start_node:
        :param end_node:
        :param edges:
        :param rel_type:
        :param rel_name:
        :return:
        """
        count = 0  # 开始结点和结束结点之间建立关系，并做好关系类型标记
        set_edges = []  # 去重处理
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            # CQL操作语句
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                self.g.run(query)  # 调用Neo4j建立关系
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
        return

    def export_data(self):
        """
        导出数据
        :return:
        """
        return


if __name__ == '__main__':
    handler = MedicalGraph()
    handler.create_graphnodes()
    handler.create_graphrels()
    handler.export_data()

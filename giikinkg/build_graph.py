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
from decimal import Decimal
from py2neo import Graph, Node
from giikinkg.nodes_product import product_nodes as read_nodes
from giikinkg.selfnodecls import MyDict


class MedicalGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        # self.data_path = os.path.join(cur_dir, '../data/medical.json')
        self.data_path = '../data/medical.json'
        self.g = Graph('http://localhost:7474/', auth=("neo4j", "aa1230.aa"))
        # self.g = Graph(
        #     host="127.0.0.1",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
        #     http_port=7474,  # neo4j 服务器监听的端口号
        #     user="neo4j",  # 数据库user name，如果没有更改过，应该是neo4j
        #     password="123456")

    # def read_nodes(self):
    #     """
    #     # 读取文件，共8类实体节点
    #     :return:
    #     """
    #     # todo 实体节点种类 - 本体种类以及存储每一类本体中包含的具体实例实体
    #     diseases = []  # 疾病
    #     drugs = []  # 药品
    #     foods = []  # 食物
    #     checks = []  # 检查项目
    #     departments = []  # 科室
    #     producers = []  # 药品大类
    #     symptoms = []  # 症状
    #     cures = []  # 治疗方法
    #
    #     # todo 构建节点实体关系
    #     rels_noteat = []  # 疾病－忌吃食物关系 （no_eat）
    #     rels_doeat = []  # 疾病－宜吃食物关系 （do_eat）
    #     rels_recommandeat = []  # 疾病－推荐吃食物关系 （recommand_eat）
    #     rels_commonddrug = []  # 疾病－通用药品关系 （common_drug）
    #     rels_recommanddrug = []  # 疾病－热门药品关系 （recommand_drug）
    #     rels_check = []  # 疾病－检查关系 （need_check）
    #     rels_cureway = []  # 疾病-治疗方式关系 （cure_way）
    #     rels_symptom = []  # 疾病症状关系 （has_symptom）
    #     rels_acompany = []  # 疾病并发关系 （acompany_with）
    #     rels_category = []  # 疾病与科室之间的关系  疾病所属科室（belongs_to）
    #     # fixme 其他实体和关系信息
    #     disease_infos = []  # 疾病信息
    #     rels_drug_producer = []  # 厂商－药物关系
    #     rels_department = []  # 科室－科室关系（前小后大）
    #
    #     # todo 从原始文件中读取【疾病】的所有字段
    #     count = 0
    #     for data in open(self.data_path, 'rb'):
    #         # todo 存储的是每一个【疾病】的所有相关信息
    #         disease_dict = {}
    #         count += 1
    #         data_json = json.loads(data)
    #         # todo entity diseases
    #         disease = data_json['name']  # important core： diseases
    #         diseases.append(disease)  # todo each data to each diseases entity  （diseases cls）｜ 一个一个【疾病】的添加
    #         # todo 疾病实体的属性 - 这里的本质是把【中心实体（疾病）的所有信息】存储在一个list中
    #         disease_dict['name'] = disease  # 疾病名称
    #         disease_dict['desc'] = ''  # 疾病简介
    #         disease_dict['prevent'] = ''  # 预防措施
    #         disease_dict['cause'] = ''  # 疾病病因
    #         disease_dict['easy_get'] = ''  # 疾病易感人群
    #         disease_dict['cure_lasttime'] = ''  # 治疗周期
    #         disease_dict['cured_prob'] = ''  # 治愈概率
    #         # fixme 其他的属性信息
    #         disease_dict['cure_department'] = ''
    #         disease_dict['cure_way'] = ''
    #         disease_dict['symptom'] = ''
    #
    #         # todo 筛选 - 每一个【中心实体-疾病】的具体的实体属性
    #         if 'desc' in data_json:
    #             disease_dict['desc'] = data_json['desc']  # todo 疾病属性 - 1
    #         if 'prevent' in data_json:
    #             disease_dict['prevent'] = data_json['prevent']  # todo 疾病属性 - 2
    #         if 'cause' in data_json:
    #             disease_dict['cause'] = data_json['cause']  # todo 疾病属性 - 3
    #         if 'get_prob' in data_json:
    #             disease_dict['get_prob'] = data_json['get_prob']  # todo 疾病属性 - 4
    #         if 'easy_get' in data_json:
    #             disease_dict['easy_get'] = data_json['easy_get']  # todo 疾病属性 - 5
    #         if 'cure_lasttime' in data_json:
    #             disease_dict['cure_lasttime'] = data_json['cure_lasttime']
    #         if 'cured_prob' in data_json:
    #             disease_dict['cured_prob'] = data_json['cured_prob']
    #
    #         # todo core disease + 其他实体以及实体之间的关系确认
    #         # todo rel - 1： 【疾病】和【症状】之间的关系
    #         if 'symptom' in data_json:
    #             symptoms += data_json['symptom']
    #             for symptom in data_json['symptom']:
    #                 rels_symptom.append([disease, symptom])
    #         # todo add rel - 2： acompany   【疾病】和并发症之间的关系  fixme： acompany 并非一种实体类型
    #         if 'acompany' in data_json:
    #             for acompany in data_json['acompany']:
    #                 rels_acompany.append([disease, acompany])
    #         # todo  add rel - 3： 【disease】 to 【department】 【疾病】和【科室】之间的关系
    #         if 'cure_department' in data_json:
    #             cure_department = data_json['cure_department']
    #             if len(cure_department) == 1:
    #                 rels_category.append([disease, cure_department[0]])
    #             if len(cure_department) == 2:
    #                 big = cure_department[0]
    #                 small = cure_department[1]
    #                 rels_department.append([small, big])
    #                 rels_category.append([disease, small])
    #             departments += cure_department
    #         # todo  add rel - 4： 【disease】 to 【cure_way】  疾病和治疗方法之间的关系
    #         if 'cure_way' in data_json:
    #             cure_way = data_json['cure_way']
    #             cures += cure_way
    #             for cure in cure_way:
    #                 rels_cureway.append([disease, cure])
    #         # todo add rel - 5 common_drug     【疾病】和【（常用）药物】之间的关系
    #         if 'common_drug' in data_json:
    #             common_drug = data_json['common_drug']
    #             for drug in common_drug:
    #                 rels_commonddrug.append([disease, drug])
    #             drugs += common_drug
    #         # todo add  rel - 6：recommand_drug   【疾病】和【（推荐）药物】之间的关系
    #         if 'recommand_drug' in data_json:
    #             recommand_drug = data_json['recommand_drug']
    #             drugs += recommand_drug
    #             for drug in recommand_drug:
    #                 rels_recommanddrug.append([disease, drug])
    #         # todo add  rel - 7+8+9： 【疾病】和【（不宜）食物】之间的关系 + 【疾病】和【（宜吃）食物】之间的关系 + 【疾病】+【（建议）食物】之间的关系
    #         if 'not_eat' in data_json:
    #             not_eat = data_json['not_eat']
    #             for _not in not_eat:
    #                 rels_noteat.append([disease, _not])
    #             foods += not_eat
    #             do_eat = data_json['do_eat']
    #             for _do in do_eat:
    #                 rels_doeat.append([disease, _do])
    #             foods += do_eat
    #             recommand_eat = data_json['recommand_eat']
    #             for _recommand in recommand_eat:
    #                 rels_recommandeat.append([disease, _recommand])
    #             foods += recommand_eat
    #         # todo rel - 10：【疾病】和【检查项目】之间的关系；
    #         if 'check' in data_json:
    #             check = data_json['check']
    #             for _check in check:
    #                 rels_check.append([disease, _check])
    #             checks += check
    #         # todo rel - 11：【疾病】和【（在售）药物】之间的关系
    #         if 'drug_detail' in data_json:
    #             drug_detail = data_json['drug_detail']
    #             producer = [i.split('(')[0] for i in drug_detail]
    #             rels_drug_producer += [[i.split('(')[0], i.split('(')[-1].replace(')', '')] for i in drug_detail]
    #             producers += producer
    #         disease_infos.append(disease_dict)
    #     return set(drugs), set(foods), set(checks), set(departments), set(producers), set(symptoms), set(diseases), \
    #            set(cures), disease_infos, rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, \
    #            rels_commonddrug, rels_drug_producer, rels_recommanddrug, rels_symptom, rels_acompany, rels_category, rels_cureway

    def create_node(self, label, nodes):
        '''建立节点'''
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.g.create(node)
            count += 1
            print(count, len(nodes))
        return

    def create_product_infos_nodes(self, disease_infos):
        '''创建知识图谱中心疾病的节点'''
        count = 0
        for disease_dict in disease_infos:
            input_dict = {}

            for k, v in disease_dict.items():
                if isinstance(v, Decimal):
                    v = str(v.quantize(Decimal('0.0')))
                    input_dict[k] = float(v)
                # if k == '投放商品':
                #     new_goods = []
                #     for eachgoods in v:
                #         tmpgoods = {}
                #         for _k, _v in eachgoods:
                #             if isinstance(v, Decimal):
                #                 _v = str(_v.quantize(Decimal('0.0')))
                #                 tmpgoods[_k] = _v
                #             else:
                #                 tmpgoods[_k] = _v
                #         new_goods.append(tmpgoods)

                # if isinstance(v, dict):
                #     input_dict[k] = str(v)

                if isinstance(v, list):
                    new_v = []
                    for _ in v:
                        if isinstance(_, dict):
                            new_v.append(str(_))
                    input_dict[k] = new_v
                else:
                    input_dict[k] = v

            # tarnode = MyDict(input_dict, 'now_node')
            # tarnode = tarnode.to_json()
            # node = Node("Product", value=tarnode)

            node = Node("Product", **input_dict)
            # node = Node("Disease", name=disease_dict['name'], desc=disease_dict['desc'],
            #             prevent=disease_dict['prevent'], cause=disease_dict['cause'], easy_get=disease_dict['easy_get'],
            #             cure_lasttime=disease_dict['cure_lasttime'], cured_prob=disease_dict['cured_prob'])
            self.g.create(node)
            count += 1
            # print(count)
        return

    def create_graphnodes(self):
        """
        创建知识图谱实体节点类型schema
        :return:
        """
        pro_names, coloryuansu_tags, size_tags, group_tags, material_tags, design_tags, function_tags, fengge_tags, sellpoint_tags, usetime_tags, \
        age_tags, selltime_tags, cat3_tags, dpgoods_tags, usescene_tags, dpcls_tags, body_tags, eat_tags, loc_tags, bof_tags, tas_tags, pubgoods_tags, \
        prt_infos, rels_prt_coloryuansu, rels_prt_size, rels_prt_groups, rels_prt_material, rels_prt_functions, rels_prt_design, rels_prt_fengge, rels_prt_sellpoint, \
        rels_prt_usetime, rels_prt_age, rels_prt_selltime, rels_prt_cat3, rels_prt_dpgoods, rels_prt_usescene, rels_prt_dpcls, rels_prt_pubgoods, rels_prt_body, rels_prt_eat, \
        rels_prt_loc, rels_prt_bof, rels_prt_tas = read_nodes()
        self.create_product_infos_nodes(prt_infos)
        self.create_node('Drug', coloryuansu_tags)
        print(len(coloryuansu_tags))
        self.create_node('Food', Foods)
        print(len(Foods))
        self.create_node('Check', Checks)
        print(len(Checks))
        self.create_node('Department', Departments)
        print(len(Departments))
        self.create_node('Producer', Producers)
        print(len(Producers))
        self.create_node('Symptom', Symptoms)
        print(len(Symptoms))
        self.create_node('Cure', Cures)
        print(len(Cures))
        return

    ''''''

    def create_graphrels(self):
        """
        创建实体关系边
        :return:
        """
        Drugs, Foods, Checks, Departments, Producers, Symptoms, Diseases, Cures, \
        disease_infos, rels_check, \
        rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, \
        rels_drug_producer, rels_recommanddrug, rels_symptom, rels_acompany, rels_category, rels_cureway = self.read_nodes()
        self.create_relationship('Disease', 'Food', rels_recommandeat, 'recommand_eat', '推荐食谱')
        self.create_relationship('Disease', 'Food', rels_noteat, 'no_eat', '忌吃')
        self.create_relationship('Disease', 'Food', rels_doeat, 'do_eat', '宜吃')
        self.create_relationship('Department', 'Department', rels_department, 'belongs_to', '属于')
        self.create_relationship('Disease', 'Drug', rels_commonddrug, 'common_drug', '常用药品')
        self.create_relationship('Producer', 'Drug', rels_drug_producer, 'drugs_of', '生产药品')
        self.create_relationship('Disease', 'Drug', rels_recommanddrug, 'recommand_drug', '好评药品')
        self.create_relationship('Disease', 'Check', rels_check, 'need_check', '诊断检查')
        self.create_relationship('Disease', 'Symptom', rels_symptom, 'has_symptom', '症状')
        self.create_relationship('Disease', 'Disease', rels_acompany, 'acompany_with', '并发症')
        self.create_relationship('Disease', 'Department', rels_category, 'belongs_to', '所属科室')
        self.create_relationship('Disease', 'Cure', rels_cureway, 'cure_way', '治疗方法')

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        """
        创建实体关联边
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

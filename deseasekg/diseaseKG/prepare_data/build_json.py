# coding: utf-8
import os
import json
from py2neo import Graph, Node


class MedicalToJson:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.data_path = os.path.join(cur_dir, '../data/medical.json')

    def read_nodes(self):
        """
        # 读取文件，共8类实体节点
        :return:
        """
        disease_infos = []  # 疾病信息
        # todo 实体节点种类 - 本体种类以及存储每一类本体中包含的具体实例实体
        diseases = []  # 疾病
        drugs = []  # 药品
        foods = []  # 食物
        checks = []  # 检查项目
        departments = []  # 科室
        producers = []  # 药品大类
        symptoms = []  # 症状
        cures = []  # 治疗方法
        # todo 构建节点实体关系
        rels_noteat = []  # 疾病－忌吃食物关系 （no_eat）
        rels_doeat = []  # 疾病－宜吃食物关系 （do_eat）
        rels_recommandeat = []  # 疾病－推荐吃食物关系 （recommand_eat）
        rels_commonddrug = []  # 疾病－通用药品关系 （common_drug）
        rels_recommanddrug = []  # 疾病－热门药品关系 （recommand_drug）
        rels_check = []  # 疾病－检查关系 （need_check）
        rels_cureway = []  # 疾病-治疗方式关系 （cure_way）
        rels_symptom = []  # 疾病症状关系 （has_symptom）
        rels_acompany = []  # 疾病并发关系 （acompany_with）
        rels_category = []  # 疾病与科室之间的关系  疾病所属科室（belongs_to）
        # fixme 其他实体和关系信息
        rels_drug_producer = []  # 厂商－药物关系
        rels_department = []  # 科室－科室关系（前小后大）

        # todo 从原始文件中读取【疾病】的所有字段
        count = 0
        for data in open(self.data_path, 'rb'):
            # todo 存储的是每一个【疾病】的所有相关信息
            disease_dict = {}
            count += 1
            data_json = json.loads(data)
            # todo entity diseases
            disease = data_json['name']  # important core： diseases
            diseases.append(disease)  # todo each data to each diseases entity  （diseases cls）｜ 一个一个【疾病】的添加
            # todo 疾病实体的属性 - 这里的本质是把【中心实体（疾病）的所有信息】存储在一个list中
            disease_dict['name'] = disease  # 疾病名称
            disease_dict['desc'] = ''  # 疾病简介
            disease_dict['prevent'] = ''  # 预防措施
            disease_dict['cause'] = ''  # 疾病病因
            disease_dict['easy_get'] = ''  # 疾病易感人群
            disease_dict['cure_lasttime'] = ''  # 治疗周期
            disease_dict['cured_prob'] = ''  # 治愈概率
            # fixme 其他的属性信息
            disease_dict['cure_department'] = ''
            disease_dict['cure_way'] = ''
            disease_dict['symptom'] = ''

            # todo 筛选 - 每一个【中心实体-疾病】的具体的实体属性
            if 'desc' in data_json:
                disease_dict['desc'] = data_json['desc']  # todo 疾病属性 - 1
            if 'prevent' in data_json:
                disease_dict['prevent'] = data_json['prevent']  # todo 疾病属性 - 2
            if 'cause' in data_json:
                disease_dict['cause'] = data_json['cause']  # todo 疾病属性 - 3
            if 'get_prob' in data_json:
                disease_dict['get_prob'] = data_json['get_prob']  # todo 疾病属性 - 4
            if 'easy_get' in data_json:
                disease_dict['easy_get'] = data_json['easy_get']  # todo 疾病属性 - 5
            if 'cure_lasttime' in data_json:
                disease_dict['cure_lasttime'] = data_json['cure_lasttime']
            if 'cured_prob' in data_json:
                disease_dict['cured_prob'] = data_json['cured_prob']

            # todo core disease + 其他实体以及实体之间的关系确认
            # todo rel - 1： 【疾病】和【症状】之间的关系
            if 'symptom' in data_json:
                symptoms += data_json['symptom']
                for symptom in data_json['symptom']:
                    rels_symptom.append([disease, symptom])
            # todo add rel - 2： acompany   【疾病】和并发症之间的关系  fixme： acompany 并非一种实体类型
            if 'acompany' in data_json:
                for acompany in data_json['acompany']:
                    rels_acompany.append([disease, acompany])
            # todo  add rel - 3： 【disease】 to 【department】 【疾病】和【科室】之间的关系
            if 'cure_department' in data_json:
                cure_department = data_json['cure_department']
                if len(cure_department) == 1:
                    rels_category.append([disease, cure_department[0]])
                if len(cure_department) == 2:
                    big = cure_department[0]
                    small = cure_department[1]
                    rels_department.append([small, big])
                    rels_category.append([disease, small])
                departments += cure_department
            # todo  add rel - 4： 【disease】 to 【cure_way】  疾病和治疗方法之间的关系
            if 'cure_way' in data_json:
                cure_way = data_json['cure_way']
                cures += cure_way
                for cure in cure_way:
                    rels_cureway.append([disease, cure])
            # todo add rel - 5 common_drug     【疾病】和【（常用）药物】之间的关系
            if 'common_drug' in data_json:
                common_drug = data_json['common_drug']
                for drug in common_drug:
                    rels_commonddrug.append([disease, drug])
                drugs += common_drug
            # todo add  rel - 6：recommand_drug   【疾病】和【（推荐）药物】之间的关系
            if 'recommand_drug' in data_json:
                recommand_drug = data_json['recommand_drug']
                drugs += recommand_drug
                for drug in recommand_drug:
                    rels_recommanddrug.append([disease, drug])
            # todo add  rel - 7+8+9： 【疾病】和【（不宜）食物】之间的关系 + 【疾病】和【（宜吃）食物】之间的关系 + 【疾病】+【（建议）食物】之间的关系
            if 'not_eat' in data_json:
                not_eat = data_json['not_eat']
                for _not in not_eat:
                    rels_noteat.append([disease, _not])
                foods += not_eat
                do_eat = data_json['do_eat']
                for _do in do_eat:
                    rels_doeat.append([disease, _do])
                foods += do_eat
                recommand_eat = data_json['recommand_eat']
                for _recommand in recommand_eat:
                    rels_recommandeat.append([disease, _recommand])
                foods += recommand_eat
            # todo rel - 10：【疾病】和【检查项目】之间的关系；
            if 'check' in data_json:
                check = data_json['check']
                for _check in check:
                    rels_check.append([disease, _check])
                checks += check
            # todo rel - 11：【疾病】和【（在售）药物】之间的关系
            if 'drug_detail' in data_json:
                drug_detail = data_json['drug_detail']
                producer = [i.split('(')[0] for i in drug_detail]
                rels_drug_producer += [[i.split('(')[0], i.split('(')[-1].replace(')', '')] for i in drug_detail]
                producers += producer
            disease_infos.append(disease_dict)
        return set(drugs), set(foods), set(checks), set(departments), set(producers), set(symptoms), set(diseases), \
               set(cures), disease_infos, rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, \
               rels_commonddrug, rels_drug_producer, rels_recommanddrug, rels_symptom, rels_acompany, rels_category, rels_cureway

    def write_node_json(self, label, nodes):
        count = 0
        en_list = []
        for node in nodes:  # 将结点集中的结点设置为
            item = {}
            item['label'] = label
            if label == 'Diseases':
                item['name'] = node['name']
                item['desc'] = node['desc']
                item['prevent'] = node['prevent']
                item['cause'] = node['cause']
                item['easy_get'] = node['easy_get']
                item['cure_lasttime'] = node['cure_lasttime']
                item['cured_prob'] = node['cured_prob']
            else:
                item['name'] = node
            count += 1
            # print(count, len(nodes))
            en_list.append(item)
        print(label, count)
        return en_list

    # todo 处理的是本体，这就意味着是处理本体的所有具体实例
    def create_nodes_json(self):
        Drugs, Foods, Checks, Departments, Producers, Symptoms, Diseases, Cures, disease_infos, rels_check, \
        rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_drug_producer, \
        rels_recommanddrug, rels_symptom, rels_acompany, rels_category, rels_cureway = self.read_nodes()
        entities = []
        # l = self.write_node_json('Disease', disease_infos)
        # entities.extend(l)
        l = self.write_node_json('Drug', Drugs)
        entities.extend(l)
        l = self.write_node_json('Food', Foods)
        entities.extend(l)
        l = self.write_node_json('Check', Checks)
        entities.extend(l)
        l = self.write_node_json('Department', Departments)
        entities.extend(l)
        l = self.write_node_json('Producer', Producers)
        entities.extend(l)
        l = self.write_node_json('Symptom', Symptoms)
        entities.extend(l)
        l = self.write_node_json('Cure', Cures)
        entities.extend(l)
        # todo 存储所有的节点信息，也就是实体信息
        filename = '../newdata/entities-.json'
        json.dump(entities, open(filename, 'w', encoding='utf-8'), indent=4, ensure_ascii=False)
        return

    def create_rels_json(self):
        Drugs, Foods, Checks, Departments, Producers, Symptoms, Diseases, Cures, disease_infos, rels_check, \
        rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, \
        rels_drug_producer, rels_recommanddrug, rels_symptom, rels_acompany, rels_category, rels_cureway = self.read_nodes()
        relations = []
        rel_set = self.write_rel_json('Disease', 'Food', rels_recommandeat, 'recommand_eat', '推荐食谱')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Disease', 'Food', rels_noteat, 'no_eat', '忌吃')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Disease', 'Food', rels_doeat, 'do_eat', '宜吃')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Department', 'Department', rels_department, 'belongs_to', '属于', '_0')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Disease', 'Department', rels_category, 'belongs_to', '所属科室', '_1')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Disease', 'Drug', rels_commonddrug, 'common_drug', '常用药品')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Producer', 'Drug', rels_drug_producer, 'drugs_of', '生产药品')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Disease', 'Drug', rels_recommanddrug, 'recommand_drug', '好评药品')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Disease', 'Check', rels_check, 'need_check', '诊断检查')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Disease', 'Symptom', rels_symptom, 'has_symptom', '症状')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Disease', 'Disease', rels_acompany, 'acompany_with', '并发症')
        relations.append(rel_set)
        rel_set = self.write_rel_json('Disease', 'Cure', rels_cureway, 'cure_way', '治疗方法')
        relations.append(rel_set)
        filename = '../newdata/relations-.json'
        json.dump(relations, open(filename, 'w', encoding='utf-8'), indent=4, ensure_ascii=False)

    def write_rel_json(self, start_node, end_node, edges, rel_type, rel_name, postfix=None):
        '''创建实体关联边'''
        # todo 创建图谱的【实体+边+实体】的框架
        rel_set = {}
        rel_set['start_entity_type'] = start_node
        rel_set['end_entity_type'] = end_node
        rel_set['rel_type'] = rel_type
        rel_set['rel_name'] = rel_name
        # todo 把具体的【实体+边+实体】填入上述框架中
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        if postfix is None:
            filename = 'newdata/' + rel_type + '.json'
        else:
            filename = 'newdata/' + rel_type + postfix + '.json'

        rels = []
        count = 0
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            item = {}
            item['start_entity_name'] = p
            item['end_entity_name'] = q
            count += 1
            rels.append(item)
        rel_set['rels'] = rels
        print(rel_type, all)
        json.dump(rel_set, open(filename, 'w', encoding='utf-8'), indent=4, ensure_ascii=False)
        return rel_set


if __name__ == '__main__':
    handler = MedicalToJson()
    handler.create_nodes_json()
    handler.create_rels_json()
    pass

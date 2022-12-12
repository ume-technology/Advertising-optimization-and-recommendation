# Advertising-optimization-and-recommendation
- 基于跨境电商各个系列数据搭建的知识图谱
- 跨境电商COD模式的广告优化以及产品推荐

## disease相关思路记录
- 参考项目：https://github.com/nuolade/disease-kb
- 关系文件：relation.json  存储的是关系数据；
- 实体文件：entity.json  本体实体有属性；其他实体无属性；但是entity.json中存储的数据信息都是本体实例；
    ###多种实体种类：
        # 第一种实体【疾病】：症状（疾病） 
              该实体具备分类：Classified   
                  ----       同理：【商品实体】Classified
              该实体具备属性：Attribute                       
                  ----       同理：【商品实体】Attribute
        # 第二种实体【Drug】
        # 第三种实体【Food】
        # 第四种实体【Department】
    ### 实体之间的关系：
        【实体】A rel_1 【实体】B；
        【实体】C rel_2 【实体】E；
        【实体】E rel_3 【实体】F；
    ### ？实体之间的关系和实体种类之间之间的关系是什么关系
    
## Giikin数据相关
- 产品商品素材等信息读取方案；
- 该项目中还有订单数据的读取方案；
- 广告系列/广告组/订单数据读取方案；
- 应该以优化师为核心:优化师的数据在 Advertising-optimization-and-recommendation - giikin高度汇总表；


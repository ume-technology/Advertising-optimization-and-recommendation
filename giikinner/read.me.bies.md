- 2022/10/21 把BIES标注的数据拉取了过来【采用BIES的方式表述NER数据】
  - 因为这个数据方便处理完为该TC模型需要的数据格式。
  - ~~因此会把这个数据统一标注为BMES数据，到时候公司的这个数据可以删除。~~
  - ~~这部分标注完成的标准的的BMES数据已经迁移到nermodelbert项目中；只是这个项目还未处理。~~
 
### 2022/10/24 关于TC数据集准备以及模型训练的工作   
- 运行该代码，首先是数据处理
  - 下面的数据清洗方式按照如下过程进行:
    1. 读取数据：
       - giikindataset\ad_material_data readdatafrommatterial.py  是读取原始的material表中的数据；
       - giikindataset\ad_material_data readadtextdatabase.pick   文件存储的读取的原始material表中的数据；
    2. 商品素材表中的广告文本的清洗：~~执行cleanadmattabledata.py代码；这个清洗脚本已经废弃~，因为这个方式处理的数据只有清洗干净广告素材数据，无法和商品ID做映射；
       当数据清洗干净后，本地化存储：readadmattabledataclean.pick；因为没有和原始的商品ID信息做映射，因此这个只存储clean text的文件已经废弃；~~
       ~~- important notice：需要注意的是：这个脚本代码里包含了对于优化师的一些数据分析；到时候把这部分数据提前出来；
       另外就是需要结合readadtextdatabase_ex.me文件，这个文件中包含了一些关于优化师的统计数据；~~
       数据清洗脚本采用了全新的方案: giikindataset\ad_material_data ad_material_data_clean.py；
       将清洗后的ad text作为一个新的字段直接添加到素材表原始的数据中，这样就是实现了清洗后的数据和商品ID的对应；
 
- 数据标注与标准的训练数据生成：
  - giikindataset\ad_material_data\clean_materialdata2ner.py 生成的数据是纯净的ad text数据，可以用来做NER标注；
  - 那么需要做的就是每次生成一批待标注的数据：存储在：giikindataset\ad_material_data\tagdata giikinnerdata.txt文件中:
    ```生成这个文件的脚本目前没有编写,用的时候把这个脚本补上来```
  - BIES标注数据 giikinnerdata.txt；
  - 执行 giikindataset\ad_material_data\checkdatalabels.py 检查 giikinnerdata.txt 文件标注的是否争取；
  - 然后把所有做完标注-check的数据统一维护在：giikindataset\ad_material_data\tagdata alldata.txt 文件中；
  - 目前还有一批之前是安装BIO方式标注的数据存储在giikindataset\ad_material_data\tagdata tmpbiotags.txt中;
    未来需要新标注数据话，把这部分数据更改为BIES数据，然后移植到alldata.txt中，那么这个newtags.txt就可以删除了； 

- 标注的标签数据的转换与数据分割
  - 这个脚本非常重要：giikindataset\ad_material_data\tagdata\convertstandardtagdata2nertraindata.py，这个脚本生成的数据，就是标准的NER标注数据：
    通过该脚本后会生成如下数据：
    - tagdata\mid_data\train.json （需要将该 train copy to raw_data\stack.json，执行splittraindata.py脚本，将stack.json进行数据切分以及将数据最终转换为TC要求的标准NER数据；） 
    - tagdata\mid_data\ner_ent2id.json
    - tagdata\mid_data\labels.json (不重要)
  - 但是为了让上述的Traindata符合TC要求的NER格式，做如下操作：
    
- 模型的训练和预测；
 
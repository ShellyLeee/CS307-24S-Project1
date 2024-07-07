# CS307-24S-Project1

南方科技大学 2024 Spring CS307 数据库原理 Project1

得分 94/100 （基础58/60+高级36/40），基础扣在ER图的两个ride部分（外键无需标识），高级扣在Data Accuracy checking的第4条（已修正）以及有部分未完成的任务。

Project 说明及要求：https://github.com/ShellyLeee/CS307-24S-Project2/blob/main/project2_intro.pdf

## 项目情况

1. 项目基本要求：
   - 基础部分（60pts）
     - 根据提供的5份深圳地铁线路相关数据，设计并绘制ER图，需满足3NF原则
     - 书写建表语句，并书写数据导入的代码，将json类型的数据导入PostgreSQL数据库
     - 书写SQL语句进行数据核验
   - 高级部分（40pts）
     - [x] 提升数据导入代码效率
     - [x] 多OS对比 
     - [x] 多编程语言对比
     - [x] 多导入数据量对比
     - [ ] 多数据库使用（如OpenGauss）

2. 可以改进部分：
   - 完成高级部分中多数据库使用的任务
   - 提升数据导入代码效率方面，目前我们使用了多线程、预编译、Batch的方法优化，只有多线程得到了大幅度优化，具体原因和解决方法还需要探索
   - 老师评语
     - Task1: ride最好作为关系用菱形表示
     - Task2: phone-number应当是varchar, 考虑现实情况是可能有字母在号码中的，且此处用bigint无实际意义，号码并非一个数字；intro应当用text

## 项目报告

https://github.com/ShellyLeee/CS307-24S-Project1/blob/main/Report-Group%201.pdf

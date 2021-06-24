
## 打包
```
mvn clean package
```

## 运行
解压后生成形如下面的文件结构
```
├── bin
│   ├── start.sh
│   └── stop.sh
├── boot
│   └── Lannister-1.0-SNAPSHOT.jar
├── conf
│   ├── AggregatorConf.yml
│   ├── application.yml
│   ├── FetcherConf.yml
│   ├── HeuristicConf.yml
│   ├── lannister-env.sh
│   ├── lannister.properties
│   ├── logback.xml
│   └── mapper
│       └── AppResultMapper.xml
├── lib
│   ├── accessors-smart-1.2.jar
│   ├── activation-1.1.1.jar
│   ├── animal-sniffer-annotations-1.17.jar
│   ├── aopalliance-repackaged-2.6.1.jar
│   ├── ...
├── sql
│   ├── ddl.sql
└── logs
```
### 建库建表
```sql
CREATE DATABASE IF NOT EXISTS lannister DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
```
将sql目录下的ddl.sql在目标mysql数据库上执行

### 配置
* 在application.yml中配置jdbc连接信息
* 在FetcherConf.yml中配置各引擎的日志拉取信息，如日志路径
* 在HeuristicConf.yml中配置各引擎的分析模块及参数信息
* 在lannister.properties中配置通用参数，如执行器数量，拉取间隔等，完整配置见Configs类
* 在lannister-env.sh中配置程序运行时的环境参数，如JVM堆大小，JVM其他参数等

### 启动执行
```
./start.sh
```

## 监控
相关的Metric通过JMX端口暴露，默认是39899，可以对接相应的监控系统

## 代码质量
mvn scalastyle:check
mvn checkstyle:check


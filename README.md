# nagatoro 長瀞

这是一个用rust开发的微服务程序。  
主要功能是监听kafaka内容，  
针对获取的json格式的mysql的binlog分析实现对其他数据的CURD操作。    

可以启动n个实例，实现对同一个kafka的topic，不同partition同时消费。  
（这里n对应partition的数量）  
例如：topic 有10个partition，那么可以启动10个微API服务。  
这样就能很快的消费数据，实现一些数据实时搬运和分析更新等业务逻辑。  
*里面的所有数据库，数据表字段都是架空的，现实中不存在。  

*  紧急修正：
1. 原来数据库连接在kafka消费者中建立会造成占用系统socket连接超限问题。
2. 信号处理在windows系统不存在编译错误问题。

これはRustで開発されたマイクロサービス・アプリケーションです。  
主な機能は、kafkaコンテンツをリッスンして、  
json形式のmysqlのbinlog分析の取得して、  
他のデータのCURD操作を実現する。  

ひとつのkafkaのトピック、  
 異なるパーティションの消費を達成するために、    
 n個のインスタンスを起動することができます。  
(ここでnはパーティションの数に対応しています）    
例：トピックは10のパーティションを持っている場合は、  
10のマイクロAPIサービスを開始することができます。  
このようにして、データを素早く消費し、  
リアルタイムでのデータ処理や更新などのビジネスロジックを実現できる。  
*そこにあるすべてのデータベース、データテーブル、  
フィールドはフェクションであり、現実には存在しない。  

*  緊急修正：
1. kafkaコンシューマでデータベース接続を確立すると、占有システムのソケット接続オーバーランの問題が発生することが判明。  
2. Windowsシステムでのシグナル処理でコンパイルエラーが発生しないようにした。  
感谢  
config = { version = "0.13.3" }  
dotenv = { version = "0.15.0" }  
` 这里这个cmake-build，需要编译环境有cmake这个东西。  
rdkafka = { version = "0.36.0", features = ["tokio", "cmake-build"] }
tokio = { version = "1.34.0", features = ["full","rt-multi-thread","macros"] }  
tokio-test = {version = "0.4.3"}  
sqlx = { version = "0.7.2", features = ["runtime-tokio", "mysql","chrono","time","bigdecimal","json","macros"] }  
serde = { version = "1.0.192", features = ["std","derive"] }  
futures = { version = "0.3.29" }  
log4rs = { version = "1.2.0" , features = ["all_components"]}  
log = { version = "0.4.20" }  
serde_json = "1.0.108"  
base64 = { version = "0.21.5" }  
md-5 = { version = "0.10.6" }  
strum_macros = { version = "0.25.3" }  
num-traits = { version = "0.2.17" }  
toml = { version = "0.8.8" }  

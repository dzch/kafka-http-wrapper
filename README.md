# kafka-http-wrapper

kafka-http-wrapper包含以下组件：

- kproxy：kafka http proxy，接受http请求，并把收到的数据写入kafka
- ktransfer：kafka http pusher，负责从kafka中消费数据，并把数据push给下游的http server
- kmonitor：ktransfer delay monitor，负责监控ktransfer的push延迟

# Build

    go get github.com/dzch/kafka-http-wrapper
	cd ${GOPATH}/src/github.com/dzch/kafka-http-wrapper
	go build kproxy/kproxy_bin.go
	go build ktransfer/ktransfer_bin.go
	go build kmonitor/kmonitor_bin.go

# Build

    git clone https://github.com/dzch/kafka-http-wrapper.git
	cd kafka-http-wrapper
	

# kproxy

## 模块设计

kproxy对上提供HTTP接口，负责把client Post来的整个msgpack写入到kafka中。uri格式为：

    http://${kproxy_ip}:${kproxy_port}/commit?topic=${topic}&key=${key}&acks=${num}

uri中：

- topic：必须传递，对应kafka中的topic
- key：可选，决定partition：
    - key不为空，则使用module routing。可以保证同一个key被串行化
	- key为空，则使用random routing
- acks：可选，参数为：
    - 0：不等待kafka响应直接返回
	- 1：等待kafka的master响应即返回成功，默认值
	- -1：等待所有的isr响应后才返回成功

HTTP Method使用POST，且Post的内容长度必须大于0。

HTTP状态码200表示提交给kafka成功，其他失败。

## 配置示例

    # kproxy.yaml

    # 日志目录
    log_dir: ./log
    # 日志等级
    #   0：不打印任何日志
    #   1：Fatal
    #   2：Warning + Fatal
    #   4：Notice + Warning + Fatal
    #   8：Trace + Notice + Warning + Fatal
    #   16：Debug + Trace + Notice + Warning + Fatal
    log_level: 4
    
    # 监听端口
    port: 5030
    # 读、写超时
    read_timeout_ms: 100
    write_timeout_ms: 100
    # req pool大小
    req_pool_size: 10240
    # producer num，1个就足够了
    producer_num: 1
    # 消息最大大小，建议 <= kafka的消息大小限制
    max_request_size_bytes: 10485760
    
    # kafka集群地址，配置部分node的ip:port就可以了
    broker_hosts:
     - 10.1.4.17:5031
     - 10.1.5.17:5031

## Run

	 ./kproxy_bin -f kproxy.yaml

# kafka

kafka的相关操作，请参照官方文档：http://kafka.apache.org/documentation.html

# ktransfer

## 模块设计 

ktransfer是为后端业务设计的一个kafka http pusher，有以下维度的抽象：

- module：
    - ktransfer可以配置多个module，各module之间互不影响
	- 一个module只能消费一个topic，对应一组后端
        - 注意：一个topic可以被多个module消费，此时每个module都拥有该topic的全部消息（相当于topic组播）
    - module的名字在一个集群内必须唯一
- worker：
    - 每个module都可以有一个或多个worker，用来给下游push数据
    - worker按照key取模来决定自己应该push的data，所以同一个key的多组数据在被push时保证串行；不同的key做并发push
- 命令点
    - 每个module都会记录一个命令点，保存该module在该topic上已消费的位置。下次启动时将从此处开始继续消费
    - 命令点的数据保存在zookeeper上

## 配置示例

主配置文件：conf/ktransfer.yaml

    # 日志目录及等级
    log_dir: ./log
    log_level: 4
    
    # 消费哪个kafka集群？
    # 这里填写对应kafka集群的zk配置，需要和该kafka集群的zk配置一致
    zk_chroot: /mq/test_cluster
    zk_timeout_ms: 3000
    zk_hosts:
     - 10.1.1.1:2181
     - 10.1.1.2:2181
     - 10.1.1.3:2181
     - 10.1.1.4:2181
     - 10.1.1.5:2181
    
    # module配置所在的目录
    module_conf_dir: ./conf/modules
    # 启用的module
    module_enabled:
     - test_mdoule_1
     - test_module_2

module配置：conf/modules/${module_name}.yaml，比如：conf/test_module_1.yaml

    # 该module消费哪个topic的消息
    topic: test_topic
    
    # 该module消费topic中那些method的消息
    # 可以不配置methods，表示消费该topic中的所有消息
    methods:
     - TestUserName
     - TestReply
    
    # 该module的后端协议，目前只支持http
    protocol:
     name: http
     # uri中{#TOPIC}、{#KEY}、{#TRANSID}依次被替换成消息中的topic、key、kafka的offset
     uri: /XXX/{#TOPIC}?key={#KEY}&transid={#TRANSID}&format=msgpack
     # headers：向下游push时额外添加的request header
     headers:
      - Host: www.myhug.cn
     # 和下游交互的读、写、连接超时
     read_timeout_ms: 0    # 不设置读超时，因为不确定后端到底要多久才能处理完毕
     write_timeout_ms: 100
     conn_timeout_ms: 100
    
    # 该module的worker数
    worker_num: 100
    # 最大已push但未确认的消息数
    # 未确认的消息数如果达到该值，则暂停push，直到有消息被确认
    window_size: 200
    # push失败后，重试次数；-1意味着无限重试直到成功
    max_retry_times: -1
    # push失败后，等待多久后再次重试
    fail_retry_interval_ms: 200
    
    # 下游集群，rr选择
    backend_servers:
     - 10.1.2.1:80
     - 10.1.2.2:80
     - 10.1.2.3:80
     - 10.1.2.4:80

## Run

	 ./ktransfer_bin -f conf/ktransfer.yaml

## 运维注意

- 可以同时启动多个ktransfer，但每个partition只会有一个ktransfer来push，并在zookeeper的协调下自动Failover
- 如果一个topic有N个partition，要保证至少有N+1个ktransfer运行（多的1个用做冗余）

# kmonitor

## 模块设计

kmonitor用来监控ktransfer的push延迟，其接口uri：

    http://${ip}:${port}/delay

使用GET方法访问，返回结果是一个json:

    {"test_cluster":{"test_module_1":{"0":12},"test_module_2":{"0":0}}}

json代表这样一个map：

    map[string][string][string] int
   
	// 第一级key：mq cluster name
	// 第二级key：ktransfer module name
	// 第三级key：该module在topic中的某个分区
	// value：延迟的消息数

	一句话说：表示各个mq集群中的ktransfer的各个module在各partation中的消费延迟

此外，针对zabbix监控，提供了一个`format=zabbix`的参数：

    curl "http://${ip}:${port}/delay?format=zabbix"

返回可以直接使用zabbix_send向zabbix server传送数据的形式：

    - test_cluster.test_module_1.0.delay 12
    - test_cluster.test_module_2.0.delay 0

## 配置示例

    # kmonitor.yaml

    # base：基本配置
    base:
     # log目录及等级
     log_dir: ./log
     log_level: 4
     # 监控的mq cluster
     clusters:
      - test_cluster
      - common_cluster
    
    # kmonitor的listening port及读写超时
    http_server:
     port: 55325
     read_timeout_ms: 100
     write_timeout_ms: 100
    
    # 被监控的mq cluster的zk配置，要和集群的zk配置一致
    test_cluster:
     chroot: /mq/test_cluster
     zk_connect_timeout_ms: 1000
     zk_session_timeout_ms: 6000
     zk_hosts:
      - 10.1.1.1:2181
      - 10.1.2.1:2181
      - 10.1.3.1:2181
      - 10.1.4.1:2181
      - 10.1.5.1:2181
    
    # 监控的mq cluster的zk配置，要和集群的zk配置一致
    common_cluster:
     chroot: /mq/common_cluster
     zk_connect_timeout_ms: 1000
     zk_session_timeout_ms: 6000
     zk_hosts:
      - 10.2.1.1:2181
      - 10.2.2.1:2181
      - 10.2.3.1:2181
      - 10.2.4.1:2181
      - 10.2.5.1:2181

## Run

	  ./kmonitor_bin -f kmonitor.yaml

# LICENSE

The MIT License (MIT)

Copyright (c) 2015 myhug.cn and zhouwench (zhouwench@gmail.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


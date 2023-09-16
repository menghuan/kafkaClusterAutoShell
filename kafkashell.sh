#!/usr/bin/env bash
# --------------------------------------------------------------
#  Author        :huazai
#  Create Time   :2023-09-16 11:16
#  Description   :Kafka 集群管理脚本
# --------------------------------------------------------------
# This script is based on the new version kafka

. ~/.bashrc
#Kafka 管理命令 start、stop、status etc
KAFKA_MANAGE_COMMAND=$@

#导入环境变量，设置 Kafka 和 Zookeeper 的路径
KAFKA_HOME=/home/src/kafka
ZK_HOME=/home/src/zk
PATH=$PATH:$KAFKA_HOME/bin:$ZK_HOME/bin

#设置 Kafka 进程的 PID 文件，如果文件夹不存在则创建。
PID_DIR="$KAFKA_HOME/pid_dir"
[ ! -d $PID_DIR ] && mkdir -p $PID_DIR

#PID file
PID_FILE="$PID_DIR/kafka.pid"

#设置 Kafka 集群中 Broker 的数量，这个需要自行根据业务需要更改。
BROKER_NUMS=3

#设置 Kafka 的启动等待时间，这个可以自行根据需要更改，默认为 60s。
[ -z $MAX_WAITING_TIME ] && MAX_WAITING_TIME=60

#设置 Kafka brokers 的地址端口。
BROKER_LIST="localhost:9091,localhost:9092,localhost:9093"

#设置 zookeeper 在 Kafka 所在的节点上的相对地址。注意：znode 节点需要跟 kafka broker  zookeeper.connect 的配置一致
ZOOKEEPERS="localhost:2181,localhost:2182,localhost:2183"

#配置sasl JAAS， zk client 运行参数加权限认证
JAAS_CONF=" -Djava.security.auth.login.config=$KAFKA_HOME/config/kafka_server_jaas.conf"
export KAFKA_HEAP_OPTS=$KAFKA_HEAP_OPTS$JAAS_CONF
export JVMFLAGS=$JVMFLAGS$JAAS_CONF


# 检查指定 PID 文件对应的 Kafka 进程是否在运行，当运行成功后返回 1
function kafkaRunning(){
    PID=0
    
    if [ ! -d $PID_DIR ]; then
        printf "Can't find pid dir.\n"
        exit 1
    fi
    
    if [ ! -f $1 ]; then
        return 0
    fi
    
    PID="$(<$1)"
    
    if [ x$PID = x ]; then
        return 0
    fi
    
    ps -p $PID > /dev/null
    
    if [ $? -eq 1 ]; then
        return 0
    else
        return 1
    fi
}

#杀死指定 PID 文件对应的 Kafka 进程，当 kill 成功返回 1 
function kafkaKill(){
    # kafka 进程id
    local localPID=$(<$1)
    local i=0
    # kill 进程
    kill $localPID 
    # 等待
    for ((i=0; i<MAX_WAITING_TIME; i++)); do
    kafkaRunning $1
    if [ $? -eq 0 ]; then return 0; fi
    sleep 1
    done
    # 终止 kafka
    kill -s KILL $localPID 
    # 等待
    for ((i=0; i<MAX_WAITING_TIME; i++)); do
    kafkaRunning $1
    if [ $? -eq 0 ]; then return 0; fi
    sleep 1
    done
    
    return 1
}

#启动 Kafka 集群
function kafkaStart(){
    printf "Starting Kafka Cluster.\n"
    local COUNT=0;
    # 循环启动 
    for((i=1;i<=$BROKER_NUMS;i++))
    do   
        #检测是否启动成功
        kafkaRunning $PID_FILE$i
        if [ $? -eq 1 ]; then
            printf "Kafka Broker$i is already running with PID=$(<$PID_FILE$i), PID_FILE=$PID_FILE$i.\n"
            ((COUNT=$COUNT+1))
            continue
        fi
        
        printf "Started Kafka Broker$i ";
        #修改 kafka 日志目录，默认路径: $KAFKA_HOME/logs
        export LOG_DIR=$KAFKA_HOME/logs/$i
        #kafka-server-start.sh -daemon $KAFKA_HOME/config/server$i.properties 
        #注意：不能捕获返后台pid
        kafka-server-start.sh $KAFKA_HOME/config/server$i.properties > /dev/null 2>&1 &
        #保存 pid
        echo $! > $PID_FILE$i
        #再次检测是否启动成功
        kafkaRunning $PID_FILE$i
        #成功计数、打印，失败打印退出
        if [ $? -eq 1 ]; then
            ((COUNT=$COUNT+1))
            printf "Success with PID=$(<$PID_FILE$i), PID_FILE=$PID_FILE$i.\n"
        else
            printf "Failed with PID=$(<$PID_FILE$i), PID_FILE=$PID_FILE$i.\n"
            break
        fi
        
    done
    #当循环启动完毕后，统计个数是否一致，如果一致打印成功，不一致打印失败
    if [ $COUNT -eq $BROKER_NUMS ]; then
        printf "Kafka Cluster Started Success.\n"
    else
        printf "Kafka Cluster Started Failed.\n"
    fi
}

#停止 Kafka 集群
function kafkaStop(){
    printf "Stopping Kafka Cluster.\n"
    local COUNT=0;
    local i=0
    # 循环停止 
    for((i=1;i<=$BROKER_NUMS;i++))
    do  
        #检测是否正在运行中
        kafkaRunning $PID_FILE$i
        if [ $? -eq 0 ]; then
            printf "Kafka broker$i is not running with PID_FILE=$PID_FILE$i.\n"
            #如果没有运行中，直接删除 pid 文件
            rm -f $PID_FILE$i
            ((COUNT=$COUNT+1))
            continue
        fi
        
        printf "Stopped Kafka broker$i ";
        #杀死 kafka 进程
        kafkaKill $PID_FILE$i
        wait
        
        if [ $? -eq 1 ]; then
            printf "failed with PID=$(<$PID_FILE$i), PID_FILE=$PID_FILE$i. \n"
        else
            printf "succeeded with PID=$(<$PID_FILE$i), PID_FILE=$PID_FILE$i.\n"
            #当进程杀死成功后，删除 pid 文件
            rm -f $PID_FILE$i
            #统计杀死进程个数
            ((COUNT=$COUNT+1))
        fi
        
    done
    #当循环停止完毕后，统计个数是否一致，如果一致打印成功，不一致打印失败
    if [ $COUNT -eq $BROKER_NUMS ]; then
        printf "Kafka Cluster Stopped Success.\n"
    else
        printf "Kafka Cluster Stopped Failed.\n"
    fi
}

#初始化 Kafka 集群，包括停止已有集群、删除 Zookeeper 中的 Kafka 节点、删除 Kafka 数据和日志、重新启动 Kafka 集群。 
function kafkaInit(){
    printf "Starting Init Kafka Cluster.\n"
    kafkaStop
    printf "Deleting Kafka znode(/kafka) from Zookeeper.\n"
    zkCli.sh -server $ZOOKEEPERS rmr /kafka 1>/dev/null #2>&1
    printf "Deleting Kafka Data And Logs from Kafka Cluster.\n"
    rm -rf $KAFKA_HOME/data $KAFKA_HOME/logs 
    kafkaStart
    printf "Init Kafka Cluster Success.\n"
}

#清理 Kafka 集群，包括停止已有集群、删除 Zookeeper 中的 Kafka 节点、删除 Kafka 数据和日志。
function kafkaClean(){
    printf "Starting Clean Kafka Cluster.\n"
    kafkaStop
    printf "Deleting Kafka znode(/kafka) from Zookeeper.\n"
    zkCli.sh -server $ZOOKEEPERS rmr /kafka 1>/dev/null #2>&1
    printf "Deleting Kafka Data And Logs from Kafka Cluster.\n"
    rm -rf $KAFKA_HOME/data $KAFKA_HOME/logs 
    printf "Clean Kafka Cluster Success.\n"
}

#查询 Kafka 集群状态，包括检查每个 Kafka broker 的运行状态。
function kafkaStatus(){
    printf "Kafka Cluster Status: \n"
    local COUNT=0;
    #循环检测
    for((i=1;i<=$BROKER_NUMS;i++))
    do  
        printf "Kafka broker$i "
        #检测是否启动成功
        kafkaRunning $PID_FILE$i
        if [ $? -eq 1 ]; then
            #如果成功计数
            ((COUNT=$COUNT+1))
            printf "is running with PID=$(<$PID_FILE$i), PID_FILE=$PID_FILE$i.\n"
        else
            printf "is not running with PID_FILE=$PID_FILE$i.\n"
        fi
    done
    #循环检测完成后，统计个数是否一致，如果一致打印成功，不一致打印失败
    if [ $COUNT -eq $BROKER_NUMS ]; then
        printf "Kafka Cluster Status is Good.\n"
    else
        printf "Kafka Cluster Status is Bad.\n"
    fi
}

#创建一个新的 Kafka 主题，需要提供副本因子、分区数和主题名称
function topicCreate(){
   kafka-topics.sh --create --bootstrap-server $BROKER_LIST --replication-factor $1 --partitions $2 --topic $3
}

#删除一个已有的 Kafka 主题，需要提供主题名称。
function topicDelete(){
    kafka-topics.sh --bootstrap-server $BROKER_LIST --delete --topic $1
}

#查看指定 Kafka 主题的详细信息，需要提供主题名称。
function topicDesc(){
    kafka-topics.sh --bootstrap-server $BROKER_LIST --describe --topic $1
}

#列出当前 Kafka 集群中所有的主题
function topicList(){
    kafka-topics.sh --list --bootstrap-server $BROKER_LIST
}

#使用控制台生产者发送消息到指定的主题。
function kafkaProducer(){
    kafka-console-producer.sh --broker-list $BROKER_LIST --topic $1
}

#使用控制台消费者从指定的主题消费消息，可选择从头开始消费或从现有消费组消费
function kafkaConsumer(){
    if [ "x$3" = "xbegin" ] ; then
        kafka-console-consumer.sh --bootstrap-server $BROKER_LIST --topic $1 --from-beginning --group $2
    else
        kafka-console-consumer.sh --bootstrap-server $BROKER_LIST --topic $1 --group $2
    fi
}

#查看指定消费者组的消费情况
function showConsumerGroup(){
    kafka-consumer-groups.sh --bootstrap-server $BROKER_LIST --describe --group $1
}

#列出所有的消费者组的详细信息，需要提供消费者组名称
function listConsumerGroups(){
    kafka-consumer-groups.sh --bootstrap-server $BROKER_LIST --list
}

# main 函数
function main {
    case "$1" in
        start)
            kafkaStart
            ;;
        stop)
            kafkaStop
            ;;
        status)
            kafkaStatus
            ;;
        init)
            kafkaInit
            ;;
        clean)
            kafkaClean
            ;;
        create)
            if [ $# -eq 4 ] ; then
                topicCreate $2 $3 $4
            else
                printf "Usage: $0 create {replics|partitions|topic}\n"
            fi
            ;;
        delete)
            if [ $# -eq 2 ] ; then
                topicDelete $2
            else
                printf "Usage: $0 delete {topic}\n"
            fi
            ;;
        topics)
            topicList
            ;;
        desc)
            if [ $# -eq 2 ] ; then
                topicDesc $2
            else
                printf "Usage: $0 desc {topic}\n"
            fi
            ;;
        producer)
            if [ $# -eq 2 ] ; then
                kafkaProducer $2
            else
                printf "Usage: $0 producer {topic}\n"
            fi
            ;;
        groups)
            listConsumerGroups
            ;;
        consumer)
            if [ $# -ge 3 ] ; then
                kafkaConsumer $2 $3 $4
            else
                printf "Usage: $0 consumer {topic|consumerGroup|[begin]}\n"
            fi
            ;;
        group)
            if [ $# -eq 2 ] ; then
                showConsumerGroup $2
            else
                printf "Usage: $0 group {topic}\n"
            fi
            ;;
        *)
            printf "Usage: $0 {start|stop|status|init|clean|create|delete|topics|desc|producer|consumer|groups|group}\n"
            ;;
    esac
}

#Starting main
main $KAFKA_MANAGER_COMMAND

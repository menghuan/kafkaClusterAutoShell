#!/usr/bin/env bash
# --------------------------------------------------------------
#  Author        :huazai
#  Create Time   :2023-09-16 11:16
#  Description   :zookeeper cluster 管理脚本
# --------------------------------------------------------------

#加载用户的 bash 配置文件。
. ~/.bashrc

#定义 ZooKeeper 的安装目录。
ZK_HOME="/home/src/zk"

#添加别名，简化执行 zkServer.sh 和 zkCli.sh 命令的操作。
alias zkServer.sh=$ZK_HOME/bin/zkServer.sh
alias zkCli.sh=$ZK_HOME/bin/zkCli.sh

#设置 ZooKeeper 集群中的服务器地址。
ZOOKEEPERS="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

#设置 ZooKeeper 集群中的节点数量。
ZK_NODES=3

#启动 ZooKeeper 集群中的所有节点。
function zkStart(){
    # 循环执行 zkServer.sh start 启动 Zookeeper
    for((i=1;i<=$ZK_NODES;i++))
    do 
        zkServer.sh start $ZK_HOME/conf/zoo$i.cfg
    done
}

#停止 ZooKeeper 集群中的所有节点。
function zkStop(){
    # 循环执行 zkServer.sh stop 停止 Zookeeper
    for((i=1;i<=$ZK_NODES;i++))
    do 
        zkServer.sh stop $ZK_HOME/conf/zoo$i.cfg
    done
}

#检查 ZooKeeper 集群中的所有节点的状态。
function zkStatus(){
    # 循环执行 zkServer.sh status 检查 Zookeeper 状态
    for((i=1;i<=$ZK_NODES;i++))
    do 
        zkServer.sh status $ZK_HOME/conf/zoo$i.cfg
    done
}

#重启 ZooKeeper 集群中的所有节点。
function zkReStart(){
    # 循环执行 zkServer.sh restart 重启 Zookeeper 
    for((i=1;i<=$ZK_NODES;i++))
    do 
        zkServer.sh restart $ZK_HOME/conf/zoo$i.cfg
    done
}

#清理 ZooKeeper 集群中的数据和日志，并重新初始化各个节点的存储目录
function zkInitClean(){
    #停止 Zookeeper 集群
    zkStop
    #清理 ZooKeeper 集群中的数据和日志
    rm -rf $ZK_HOME/data/*
    rm -rf $ZK_HOME/logs/*
    #重新初始化各个节点的存储目录
    for((i=1;i<=$ZK_NODES;i++))
    do  
        mkdir -p $ZK_HOME/data/$i
        echo $i > $ZK_HOME/data/$i/myid
    done
}

#初始化并启动 ZooKeeper 集群
function zkInit(){
    zkInitClean
    zkStart
}

#连接到 ZooKeeper 集群的客户端应用。
function zkConnCluster(){
    zkCli.sh -server $ZOOKEEPERS
}


case "$1" in
    start)
        zkStart
        ;;
    stop)
        zkStop
        ;;
    status)
        zkStatus
        ;;
    restart)
        zkReStart
        ;;
    conn)
        zkConnCluster
        ;;
    clean)
        zkInitClean
        ;;
    init)
        zkInit
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|init|clean|conn}"
        RETVAL=1
esac

#退出脚本
exit $RETVAL

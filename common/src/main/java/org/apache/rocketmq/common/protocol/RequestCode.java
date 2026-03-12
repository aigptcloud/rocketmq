/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.protocol;

/**
 * RocketMQ 协议请求码定义类。
 * <p>
 * 该类定义了客户端与 Broker、NameServer 之间通信的所有请求类型，
 * 每个请求码对应一种特定的操作。
 * </p>
 */
public class RequestCode {

    // ==================== 消息发送与消费 (10-19) ====================

    /**
     * 发送消息
     */
    public static final int SEND_MESSAGE = 10;

    /**
     * 拉取消息
     */
    public static final int PULL_MESSAGE = 11;

    /**
     * 根据消息 Key 查询消息
     */
    public static final int QUERY_MESSAGE = 12;

    /**
     * 查询 Broker 中消费队列的偏移量
     */
    public static final int QUERY_BROKER_OFFSET = 13;

    /**
     * 查询消费者的消费进度
     */
    public static final int QUERY_CONSUMER_OFFSET = 14;

    /**
     * 更新消费者的消费进度
     */
    public static final int UPDATE_CONSUMER_OFFSET = 15;

    /**
     * 更新或创建主题
     */
    public static final int UPDATE_AND_CREATE_TOPIC = 17;

    /**
     * 获取所有主题配置
     */
    public static final int GET_ALL_TOPIC_CONFIG = 21;

    /**
     * 获取主题配置列表
     */
    public static final int GET_TOPIC_CONFIG_LIST = 22;

    /**
     * 获取主题名称列表
     */
    public static final int GET_TOPIC_NAME_LIST = 23;

    /**
     * 更新 Broker 配置
     */
    public static final int UPDATE_BROKER_CONFIG = 25;

    /**
     * 获取 Broker 配置
     */
    public static final int GET_BROKER_CONFIG = 26;

    /**
     * 触发删除过期文件
     */
    public static final int TRIGGER_DELETE_FILES = 27;

    /**
     * 获取 Broker 运行时信息
     */
    public static final int GET_BROKER_RUNTIME_INFO = 28;

    /**
     * 根据时间戳查询消费队列偏移量
     */
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;

    /**
     * 获取消费队列最大偏移量
     */
    public static final int GET_MAX_OFFSET = 30;

    /**
     * 获取消费队列最小偏移量
     */
    public static final int GET_MIN_OFFSET = 31;

    /**
     * 获取最早消息的存储时间
     */
    public static final int GET_EARLIEST_MSG_STORETIME = 32;

    /**
     * 根据消息 ID 查询消息
     */
    public static final int VIEW_MESSAGE_BY_ID = 33;

    /**
     * 客户端发送心跳
     */
    public static final int HEART_BEAT = 34;

    /**
     * 注销客户端
     */
    public static final int UNREGISTER_CLIENT = 35;

    /**
     * 消费者发送消息回退（重试）
     */
    public static final int CONSUMER_SEND_MSG_BACK = 36;

    /**
     * 结束事务（提交或回滚）
     */
    public static final int END_TRANSACTION = 37;

    /**
     * 获取消费者组中的消费者列表
     */
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;

    /**
     * 检查事务状态
     */
    public static final int CHECK_TRANSACTION_STATE = 39;

    /**
     * 通知消费者 ID 发生变化
     */
    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;

    /**
     * 批量锁定消息队列
     */
    public static final int LOCK_BATCH_MQ = 41;

    /**
     * 批量解锁消息队列
     */
    public static final int UNLOCK_BATCH_MQ = 42;

    /**
     * 获取所有消费者的消费进度
     */
    public static final int GET_ALL_CONSUMER_OFFSET = 43;

    /**
     * 获取所有延迟级别偏移量
     */
    public static final int GET_ALL_DELAY_OFFSET = 45;

    /**
     * 检查客户端配置
     */
    public static final int CHECK_CLIENT_CONFIG = 46;

    // ==================== ACL 权限控制 (50-59) ====================

    /**
     * 更新或创建 ACL 配置
     */
    public static final int UPDATE_AND_CREATE_ACL_CONFIG = 50;

    /**
     * 删除 ACL 配置
     */
    public static final int DELETE_ACL_CONFIG = 51;

    /**
     * 获取 Broker 集群 ACL 信息
     */
    public static final int GET_BROKER_CLUSTER_ACL_INFO = 52;

    /**
     * 更新全局白名单地址配置
     */
    public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53;

    /**
     * 获取 Broker 集群 ACL 配置
     */
    public static final int GET_BROKER_CLUSTER_ACL_CONFIG = 54;

    // ==================== NameServer 服务 (100-109) ====================

    /**
     * 添加 KV 配置
     */
    public static final int PUT_KV_CONFIG = 100;

    /**
     * 获取 KV 配置
     */
    public static final int GET_KV_CONFIG = 101;

    /**
     * 删除 KV 配置
     */
    public static final int DELETE_KV_CONFIG = 102;

    /**
     * 注册 Broker
     */
    public static final int REGISTER_BROKER = 103;

    /**
     * 注销 Broker
     */
    public static final int UNREGISTER_BROKER = 104;

    /**
     * 根据主题获取路由信息
     */
    public static final int GET_ROUTEINTO_BY_TOPIC = 105;

    /**
     * 获取 Broker 集群信息
     */
    public static final int GET_BROKER_CLUSTER_INFO = 106;

    // ==================== 订阅组管理 (200-219) ====================

    /**
     * 更新或创建订阅组
     */
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;

    /**
     * 获取所有订阅组配置
     */
    public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;

    /**
     * 获取主题统计信息
     */
    public static final int GET_TOPIC_STATS_INFO = 202;

    /**
     * 获取消费者连接列表
     */
    public static final int GET_CONSUMER_CONNECTION_LIST = 203;

    /**
     * 获取生产者连接列表
     */
    public static final int GET_PRODUCER_CONNECTION_LIST = 204;

    /**
     * 清除 Broker 的写权限
     */
    public static final int WIPE_WRITE_PERM_OF_BROKER = 205;

    /**
     * 从 NameServer 获取所有主题列表
     */
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;

    /**
     * 删除订阅组
     */
    public static final int DELETE_SUBSCRIPTIONGROUP = 207;

    /**
     * 获取消费统计信息
     */
    public static final int GET_CONSUME_STATS = 208;

    /**
     * 暂停消费者
     */
    public static final int SUSPEND_CONSUMER = 209;

    /**
     * 恢复消费者
     */
    public static final int RESUME_CONSUMER = 210;

    /**
     * 在消费者端重置消费偏移量
     */
    public static final int RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;

    /**
     * 在 Broker 端重置消费偏移量
     */
    public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;

    /**
     * 调整消费者线程池
     */
    public static final int ADJUST_CONSUMER_THREAD_POOL = 213;

    /**
     * 查询谁在消费该消息
     */
    public static final int WHO_CONSUME_THE_MESSAGE = 214;

    /**
     * 在 Broker 中删除主题
     */
    public static final int DELETE_TOPIC_IN_BROKER = 215;

    /**
     * 在 NameServer 中删除主题
     */
    public static final int DELETE_TOPIC_IN_NAMESRV = 216;

    /**
     * 根据命名空间获取 KV 列表
     */
    public static final int GET_KVLIST_BY_NAMESPACE = 219;

    /**
     * 重置消费者客户端偏移量
     */
    public static final int RESET_CONSUMER_CLIENT_OFFSET = 220;

    /**
     * 从客户端获取消费者状态
     */
    public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 221;

    /**
     * 调用 Broker 重置偏移量
     */
    public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;

    /**
     * 调用 Broker 获取消费者状态
     */
    public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;

    /**
     * 根据集群获取主题列表
     */
    public static final int GET_TOPICS_BY_CLUSTER = 224;

    // ==================== 消息过滤与查询 (300-309) ====================

    /**
     * 查询谁在消费该主题
     */
    public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;

    /**
     * 注册过滤服务器
     */
    public static final int REGISTER_FILTER_SERVER = 301;

    /**
     * 注册消息过滤类
     */
    public static final int REGISTER_MESSAGE_FILTER_CLASS = 302;

    /**
     * 查询消费时间跨度
     */
    public static final int QUERY_CONSUME_TIME_SPAN = 303;

    /**
     * 从 NameServer 获取系统主题列表
     */
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;

    /**
     * 从 Broker 获取系统主题列表
     */
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;

    /**
     * 清理过期消费队列
     */
    public static final int CLEAN_EXPIRED_CONSUMEQUEUE = 306;

    /**
     * 获取消费者运行时信息
     */
    public static final int GET_CONSUMER_RUNNING_INFO = 307;

    /**
     * 查询校正偏移量
     */
    public static final int QUERY_CORRECTION_OFFSET = 308;

    /**
     * 直接消费消息（用于管理命令）
     */
    public static final int CONSUME_MESSAGE_DIRECTLY = 309;

    // ==================== 消息发送扩展 (310-326) ====================

    /**
     * 发送消息 V2（优化版本）
     */
    public static final int SEND_MESSAGE_V2 = 310;

    /**
     * 获取单元化主题列表
     */
    public static final int GET_UNIT_TOPIC_LIST = 311;

    /**
     * 获取有单元化订阅的主题列表
     */
    public static final int GET_HAS_UNIT_SUB_TOPIC_LIST = 312;

    /**
     * 获取有单元化订阅的非单元化主题列表
     */
    public static final int GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;

    /**
     * 克隆消费者组偏移量
     */
    public static final int CLONE_GROUP_OFFSET = 314;

    /**
     * 查看 Broker 统计数据
     */
    public static final int VIEW_BROKER_STATS_DATA = 315;

    /**
     * 清理未使用的主题
     */
    public static final int CLEAN_UNUSED_TOPIC = 316;

    /**
     * 获取 Broker 消费统计
     */
    public static final int GET_BROKER_CONSUME_STATS = 317;

    /**
     * 更新 NameServer 配置
     */
    public static final int UPDATE_NAMESRV_CONFIG = 318;

    /**
     * 获取 NameServer 配置
     */
    public static final int GET_NAMESRV_CONFIG = 319;

    /**
     * 批量发送消息
     */
    public static final int SEND_BATCH_MESSAGE = 320;

    /**
     * 查询消费队列
     */
    public static final int QUERY_CONSUME_QUEUE = 321;

    /**
     * 查询数据版本
     */
    public static final int QUERY_DATA_VERSION = 322;

    /**
     * 恢复检查已放入 TRANS_CHECK_MAXTIME_TOPIC 的半消息
     */
    public static final int RESUME_CHECK_HALF_MESSAGE = 323;

    /**
     * 发送回复消息
     */
    public static final int SEND_REPLY_MESSAGE = 324;

    /**
     * 发送回复消息 V2
     */
    public static final int SEND_REPLY_MESSAGE_V2 = 325;

    /**
     * 推送回复消息到客户端
     */
    public static final int PUSH_REPLY_MESSAGE_TO_CLIENT = 326;
}

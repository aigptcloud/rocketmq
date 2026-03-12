# RocketMQ 长轮询（Long Polling）机制详解

## 一、概述

RocketMQ 的 Push 消费模式底层基于 **长轮询（Long Polling）** 实现。长轮询结合了 Pull 模式的资源节省和 Push 模式的实时性，在消息不堆积时可以实现准实时的消息推送。

### 三种消息获取模式对比

| 模式 | 实时性 | 资源消耗 | 实现复杂度 | 适用场景 |
|------|--------|----------|------------|----------|
| **短轮询（Short Polling）** | 差（有延迟） | 高（频繁请求） | 简单 | 不推荐使用 |
| **长轮询（Long Polling）** | 好（准实时） | 低（连接挂起） | 中等 | RocketMQ 默认 |
| **真正的 Push** | 最好 | 中（长连接） | 复杂 | 需要持久连接 |

---

## 二、长轮询核心原理

### 2.1 工作流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         长轮询工作流程                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Consumer                              Broker                              │
│      │                                     │                                │
│      │  1. 发起拉取请求（带SUSPEND标志）    │                                │
│      │     suspendTimeoutMillis = 15000ms   │                                │
│      ├─────────────────────────────────────►│                                │
│      │                                     │                                │
│      │                              ┌──────▼──────┐                        │
│      │                              │ PullMessage │                        │
│      │                              │  Processor  │                        │
│      │                              └──────┬──────┘                        │
│      │                                     │                                │
│      │                              ┌──────▼──────┐                        │
│      │                              │ 检查是否有新消息 │                      │
│      │                              └──────┬──────┘                        │
│      │                                     │                                │
│      │         ┌───────────────────────────┼───────────────────┐           │
│      │         │                           │                   │           │
│      │         ▼                           ▼                   ▼           │
│      │    【有新消息】                 【无新消息】               │           │
│      │         │                           │                   │           │
│      │         │ 立即返回消息               │ 挂起请求到         │           │
│      │         │                           │ PullRequestHoldService        │
│      │◄────────┘                           │                   │           │
│      │                                     │                   │           │
│      │         ┌───────────────────────────┘                   │           │
│      │         │                                               │           │
│      │         │ 2. 消息到达触发通知（ReputMessageService）      │           │
│      │         │◄──────────────────────────────────────────────┘           │
│      │         │                                               │           │
│      │         │ 3. 被挂起的请求被唤醒，执行拉取                │           │
│      │         ├──────────────────────────────────────────────►│           │
│      │         │                                               │           │
│      │◄────────┘ 返回新消息                                     │           │
│      │                                                         │           │
│      │ 4. 超时处理（15秒内无新消息）                            │           │
│      │◄───────────────────────────────────────────────────────┘           │
│      │    返回 PULL_NOT_FOUND，客户端立即重新发起请求                      │
│      │                                                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 关键设计

**核心思想**：Broker 在暂时没有新消息时，不立即返回空结果，而是将请求挂起。当新消息到达或超时（默认15秒）时，再返回给客户端。

```java
// 客户端关键参数
private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;  // Broker 最大挂起时间 15秒
private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;  // 客户端超时 30秒
```

---

## 三、Broker 端实现

### 3.1 核心组件

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Broker 长轮询组件                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────┐                                                    │
│  │ PullMessageProcessor │ ←── 处理客户端拉取请求                            │
│  └──────────┬──────────┘                                                    │
│             │                                                               │
│             │ 无消息时挂起请求                                               │
│             ▼                                                               │
│  ┌─────────────────────┐     ┌─────────────────────┐                        │
│  │ PullRequestHoldService│◄────│ ManyPullRequest      │                        │
│  │     (挂起服务)       │     │ (每个队列的请求列表)  │                        │
│  └──────────┬──────────┘     └─────────────────────┘                        │
│             │                                                               │
│             │ 消息到达时通知                                                 │
│             ▼                                                               │
│  ┌─────────────────────┐                                                    │
│  │ NotifyMessageArriving│ ←── 消息到达监听器                                 │
│  │      Listener        │                                                    │
│  └─────────────────────┘                                                    │
│                                                                             │
│  ┌─────────────────────┐                                                    │
│  │  ReputMessageService │ ←── 异步分发消息时触发通知                         │
│  │   (消息分发服务)      │                                                    │
│  └─────────────────────┘                                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 PullMessageProcessor 处理拉取请求

```java
// PullMessageProcessor.java
private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
                                        boolean brokerAllowSuspend) {
    // ... 解析请求参数 ...

    final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
    final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

    // 从存储层获取消息
    final GetMessageResult getMessageResult = this.brokerController.getMessageStore()
        .getMessage(requestHeader.getConsumerGroup(), topic, queueId,
                    requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), messageFilter);

    switch (getMessageResult.getStatus()) {
        case FOUND:
            // 找到消息，立即返回
            response.setCode(ResponseCode.SUCCESS);
            break;

        case NO_MATCHED_MESSAGE:
        case NO_MESSAGE_IN_QUEUE:
        case OFFSET_OVERFLOW_ONE:
            // 没有匹配的消息
            response.setCode(ResponseCode.PULL_NOT_FOUND);

            // ★★★ 关键：如果允许挂起且有挂起标志，则将请求挂起 ★★★
            if (brokerAllowSuspend && hasSuspendFlag) {
                long pollingTimeMills = suspendTimeoutMillisLong;

                // 如果 Broker 禁用长轮询，则使用短轮询时间（默认1秒）
                if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                }

                // 创建挂起请求
                PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills,
                    this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);

                // 将请求交给 HoldService 挂起
                this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);

                // 返回 null，表示暂不响应，等待被唤醒或超时
                return null;
            }
            break;
    }
    // ...
}
```

### 3.3 PullRequestHoldService 挂起服务

```java
// PullRequestHoldService.java
public class PullRequestHoldService extends ServiceThread {
    // key: topic@queueId，value: 该队列的所有挂起请求
    private ConcurrentMap<String, ManyPullRequest> pullRequestTable =
        new ConcurrentMap<String, ManyPullRequest>(1024);

    // 挂起请求：将请求暂存到内存
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }
        mpr.addPullRequest(pullRequest);
    }

    // 后台线程：定期检查挂起的请求
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    // 长轮询模式：每 5 秒检查一次
                    this.waitForRunning(5 * 1000);
                } else {
                    // 短轮询模式：按配置的短轮询时间检查
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                // 检查挂起的请求
                this.checkHoldRequest();
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
    }

    // 定期检查：看是否有新消息到达
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);

                // 获取该队列当前最大偏移量
                final long offset = this.brokerController.getMessageStore()
                    .getMaxOffsetInQueue(topic, queueId);

                // 通知消息到达（可能唤醒挂起的请求）
                this.notifyMessageArriving(topic, queueId, offset);
            }
        }
    }
}
```

### 3.4 消息到达通知机制

```java
// PullRequestHoldService.java
public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset,
                                   Long tagsCode, long msgStoreTime,
                                   byte[] filterBitMap, Map<String, String> properties) {
    String key = this.buildKey(topic, queueId);
    ManyPullRequest mpr = this.pullRequestTable.get(key);
    if (mpr != null) {
        // 取出所有挂起的请求（清空列表）
        List<PullRequest> requestList = mpr.cloneListAndClear();
        if (requestList != null) {
            List<PullRequest> replayList = new ArrayList<PullRequest>();

            for (PullRequest request : requestList) {
                long newestOffset = maxOffset;

                // 再次检查是否有新消息（双重检查）
                if (newestOffset <= request.getPullFromThisOffset()) {
                    newestOffset = this.brokerController.getMessageStore()
                        .getMaxOffsetInQueue(topic, queueId);
                }

                // 有新消息到达
                if (newestOffset > request.getPullFromThisOffset()) {
                    // 检查消息是否匹配过滤条件（Tag/SQL92）
                    boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                        new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));

                    if (match && properties != null) {
                        match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                    }

                    if (match) {
                        try {
                            // ★★★ 唤醒请求：重新执行拉取 ★★★
                            this.brokerController.getPullMessageProcessor()
                                .executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;  // 已处理，不放入 replayList
                    }
                }

                // 检查是否超时
                if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                    try {
                        // 超时：立即返回空结果
                        this.brokerController.getPullMessageProcessor()
                            .executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                    } catch (Throwable e) {
                        log.error("execute request when wakeup failed.", e);
                    }
                    continue;  // 已处理，不放入 replayList
                }

                // 既没有新消息，也没超时：重新放回挂起列表
                replayList.add(request);
            }

            // 未处理的请求重新挂起
            if (!replayList.isEmpty()) {
                mpr.addPullRequest(replayList);
            }
        }
    }
}
```

### 3.5 消息写入时触发通知

```java
// ReputMessageService.java (在 DefaultMessageStore 内部)
class ReputMessageService extends ServiceThread {
    private void doReput() {
        // 从 CommitLog 读取新写入的消息
        SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);

        if (result != null) {
            for (int readSize = 0; readSize < result.getSize(); ) {
                // 解析消息
                DispatchRequest dispatchRequest = DefaultMessageStore.this.commitLog
                    .checkMessageAndReturnSize(result.getByteBuffer(), false, false);

                if (dispatchRequest.isSuccess()) {
                    // 分发到 ConsumeQueue 和 IndexFile
                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                    // ★★★ 关键：触发长轮询通知 ★★★
                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                        && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {

                        DefaultMessageStore.this.messageArrivingListener.arriving(
                            dispatchRequest.getTopic(),
                            dispatchRequest.getQueueId(),
                            dispatchRequest.getConsumeQueueOffset() + 1,
                            dispatchRequest.getTagsCode(),
                            dispatchRequest.getStoreTimestamp(),
                            dispatchRequest.getBitMap(),
                            dispatchRequest.getPropertiesMap());
                    }
                }
            }
        }
    }
}
```

### 3.6 唤醒后重新执行拉取

```java
// PullMessageProcessor.java
public void executeRequestWhenWakeup(final Channel channel, final RemotingCommand request)
    throws RemotingCommandException {

    Runnable run = new Runnable() {
        @Override
        public void run() {
            try {
                // 重新执行拉取，这次 brokerAllowSuspend = false（不再次挂起）
                final RemotingCommand response = PullMessageProcessor.this
                    .processRequest(channel, request, false);

                if (response != null) {
                    response.setOpaque(request.getOpaque());
                    response.markResponseType();

                    // 发送响应给客户端
                    channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                log.error("processRequestWrapper response to {} failed",
                                    future.channel().remoteAddress(), future.cause());
                            }
                        }
                    });
                }
            } catch (RemotingCommandException e1) {
                log.error("excuteRequestWhenWakeup run", e1);
            }
        }
    };

    // 提交到线程池异步执行
    this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, request));
}
```

---

## 四、客户端实现

### 4.1 客户端发起长轮询

```java
// DefaultMQPushConsumerImpl.java
public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    // 关键参数
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;  // Broker 挂起最长时间
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;  // 客户端超时

    private void pullMessage(final PullRequest pullRequest) {
        final long beginTimestamp = System.currentTimeMillis();

        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                // 处理拉取结果...
            }

            @Override
            public void onException(Throwable e) {
                // 异常处理，延迟后重试
                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            }
        };

        // 构建拉取请求
        int sysFlag = PullSysFlag.buildSysFlag(
            commitOffsetEnable,    // 是否提交消费进度
            true,                  // ★★★ 启用挂起（长轮询）★★★
            subExpression != null, // 是否有订阅表达式
            classFilter            // 是否类过滤
        );

        try {
            // 发起拉取请求
            this.pullAPIWrapper.pullKernelImpl(
                pullRequest.getMessageQueue(),           // 消息队列
                subExpression,                           // 订阅表达式
                subscriptionData.getExpressionType(),    // 表达式类型（TAG/SQL92）
                subscriptionData.getSubVersion(),        // 订阅版本
                pullRequest.getNextOffset(),             // 拉取偏移量
                32,                                      // 最大消息数
                sysFlag,                                 // 系统标志（包含挂起标志）
                commitOffsetValue,                       // 提交的消费进度
                BROKER_SUSPEND_MAX_TIME_MILLIS,          // ★★★ Broker 最大挂起时间 15秒 ★★★
                CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,    // ★★★ 客户端超时 30秒 ★★★
                CommunicationMode.ASYNC,                 // 异步模式
                pullCallback                             // 回调
            );
        } catch (Exception e) {
            // 异常处理...
        }
    }
}
```

### 4.2 PullSysFlag 标志位定义

```java
// PullSysFlag.java
public class PullSysFlag {
    private final static int FLAG_COMMIT_OFFSET = 0x1;       // 提交消费进度
    private final static int FLAG_SUSPEND = 0x1 << 1;        // ★ 允许挂起（长轮询）
    private final static int FLAG_SUBSCRIPTION = 0x1 << 2;   // 携带订阅数据
    private final static int FLAG_CLASS_FILTER = 0x1 << 3;   // 类过滤模式

    public static int buildSysFlag(boolean commitOffset, boolean suspend,
                                    boolean subscription, boolean classFilter) {
        int flag = 0;
        if (commitOffset) flag |= FLAG_COMMIT_OFFSET;
        if (suspend) flag |= FLAG_SUSPEND;        // 设置挂起标志
        if (subscription) flag |= FLAG_SUBSCRIPTION;
        if (classFilter) flag |= FLAG_CLASS_FILTER;
        return flag;
    }

    public static boolean hasSuspendFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUSPEND) == FLAG_SUSPEND;
    }
}
```

### 4.3 网络层请求封装

```java
// PullAPIWrapper.java
public PullResult pullKernelImpl(
    MessageQueue mq,
    String subExpression,
    String expressionType,
    long subVersion,
    long offset,
    int maxNums,
    int sysFlag,                        // 系统标志
    long commitOffset,
    long brokerSuspendMaxTimeMillis,    // Broker 最大挂起时间
    long timeoutMillis,                 // 客户端超时时间
    CommunicationMode communicationMode,
    PullCallback pullCallback
) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(...);

    // 构建请求头
    PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
    requestHeader.setConsumerGroup(this.consumerGroup);
    requestHeader.setTopic(mq.getTopic());
    requestHeader.setQueueId(mq.getQueueId());
    requestHeader.setQueueOffset(offset);
    requestHeader.setMaxMsgNums(maxNums);
    requestHeader.setSysFlag(sysFlag);                           // 传递标志位
    requestHeader.setCommitOffset(commitOffset);
    requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);  // 挂起超时时间
    requestHeader.setSubscription(subExpression);
    requestHeader.setSubVersion(subVersion);

    // 发送请求
    PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
        brokerAddr, requestHeader, timeoutMillis, communicationMode, pullCallback);

    return pullResult;
}
```

---

## 五、完整流程时序图

```
┌─────────┐     ┌─────────────┐     ┌──────────────────┐     ┌─────────────────────┐     ┌──────────────┐
│ Consumer│     │PullAPIWrapper│     │PullMessageProcessor│   │PullRequestHoldService│   │ReputMessageService│
└────┬────┘     └──────┬──────┘     └────────┬─────────┘     └──────────┬──────────┘     └──────┬───────┘
     │                 │                      │                          │                      │
     │ 1. pullMessage  │                      │                          │                      │
     │────────────────►│                      │                          │                      │
     │                 │                      │                          │                      │
     │                 │ 2. pullKernelImpl    │                          │                      │
     │                 │  (suspendFlag=true,  │                          │                      │
     │                 │   timeout=15s)       │                          │                      │
     │                 │─────────────────────►│                          │                      │
     │                 │                      │                          │                      │
     │                 │                      │ 3. 检查是否有新消息         │                      │
     │                 │                      │    getMessage()          │                      │
     │                 │                      │                          │                      │
     │                 │                      │ 4. 返回 NO_MESSAGE       │                      │
     │                 │                      │                          │                      │
     │                 │                      │ 5. suspendPullRequest    │                      │
     │                 │                      │─────────────────────────►│                      │
     │                 │                      │                          │                      │
     │                 │                      │ 6. return null           │                      │
     │                 │                      │  (请求被挂起，不立即响应)  │                      │
     │                 │◄─────────────────────│                          │                      │
     │                 │                      │                          │                      │
     │                 │ 7. 客户端连接保持等待  │                          │                      │
     │◄────────────────│                      │                          │                      │
     │                 │                      │                          │                      │
     │                 │                      │                          │                      │
     │                 │                      │                          │◄─────────────────────│
     │                 │                      │                          │ 8. 新消息到达          │
     │                 │                      │                          │   notifyMessageArriving
     │                 │                      │                          │                      │
     │                 │                      │ 9. executeRequestWhenWakeup│                     │
     │                 │                      │◄─────────────────────────│                      │
     │                 │                      │                          │                      │
     │                 │                      │ 10. 重新执行拉取           │                      │
     │                 │                      │    getMessage()          │                      │
     │                 │                      │                          │                      │
     │                 │                      │ 11. 返回 FOUND           │                      │
     │                 │                      │                          │                      │
     │                 │ 12. 返回消息给客户端   │                          │                      │
     │◄────────────────│◄─────────────────────│                          │                      │
     │                 │                      │                          │                      │
     │ 13. 处理消息      │                      │                          │                      │
     │                 │                      │                          │                      │
```

---

## 六、关键设计要点

### 6.1 为什么客户端超时（30秒） > Broker 挂起时间（15秒）？

```java
// 客户端超时 30 秒
CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;

// Broker 挂起时间 15 秒
BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
```

**原因**：
1. **Broker 先超时**：Broker 在 15 秒后会主动返回 `PULL_NOT_FOUND`
2. **防止客户端误判**：客户端需要等待 Broker 的响应，如果客户端超时太短，可能在 Broker 准备返回时客户端已经断开
3. **网络延迟缓冲**：30 秒的超时给网络传输留有余地

### 6.2 为什么要双重检查（Double Check）？

```java
// notifyMessageArriving 中的双重检查
long newestOffset = maxOffset;
if (newestOffset <= request.getPullFromThisOffset()) {
    // 再次查询，防止消息在通知和查询之间被消费
    newestOffset = this.brokerController.getMessageStore()
        .getMaxOffsetInQueue(topic, queueId);
}
```

**原因**：
- 消息可能在通知发出后被其他消费者消费
- 确保真的有新消息可拉取才唤醒请求

### 6.3 短轮询 vs 长轮询

```java
// Broker 配置
private boolean longPollingEnable = true;  // 默认开启长轮询
private int shortPollingTimeMills = 1000;  // 短轮询等待 1 秒

// 在 PullRequestHoldService 中
if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
    this.waitForRunning(5 * 1000);  // 长轮询：每 5 秒检查
} else {
    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
    // 短轮询：每 1 秒检查
}
```

| 模式 | Broker 行为 | 实时性 | 资源消耗 |
|------|------------|--------|----------|
| 长轮询 | 请求挂起，消息到达立即通知 | 毫秒级 | 低（连接复用）|
| 短轮询 | 等待 1 秒后返回 | 秒级 | 中（频繁请求）|

---

## 七、源码文件位置

| 文件 | 路径 | 说明 |
|------|------|------|
| PullRequestHoldService | `broker/src/main/java/org/apache/rocketmq/broker/longpolling/PullRequestHoldService.java` | 长轮询核心服务 |
| ManyPullRequest | `broker/src/main/java/org/apache/rocketmq/broker/longpolling/ManyPullRequest.java` | 挂起请求列表 |
| PullRequest | `broker/src/main/java/org/apache/rocketmq/broker/longpolling/PullRequest.java` | 挂起请求封装 |
| NotifyMessageArrivingListener | `broker/src/main/java/org/apache/rocketmq/broker/longpolling/NotifyMessageArrivingListener.java` | 消息到达监听器 |
| PullMessageProcessor | `broker/src/main/java/org/apache/rocketmq/broker/processor/PullMessageProcessor.java` | 拉取请求处理器 |
| PullSysFlag | `common/src/main/java/org/apache/rocketmq/common/sysflag/PullSysFlag.java` | 拉取标志位定义 |
| DefaultMQPushConsumerImpl | `client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java` | 客户端 Push 实现 |
| PullAPIWrapper | `client/src/main/java/org/apache/rocketmq/client/impl/consumer/PullAPIWrapper.java` | 客户端拉取封装 |
| ReputMessageService | `store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java` (内部类) | 消息分发服务 |
| MessageArrivingListener | `store/src/main/java/org/apache/rocketmq/store/MessageArrivingListener.java` | 消息到达监听接口 |

---

## 八、总结

RocketMQ 的长轮询机制实现了**"准实时 Push"**的效果：

1. **客户端角度**：像 Push 一样实时收到消息（延迟通常在毫秒级）
2. **服务端角度**：实际是 Pull 模式，资源消耗低，Broker 控制节奏
3. **核心机制**：
   - 无消息时挂起请求（不立即返回空）
   - 新消息到达时主动通知
   - 超时机制防止请求无限挂起

这种设计在保证实时性的同时，避免了真正的 Push 模式所需的复杂连接管理和流控逻辑，是一种工程上的优秀折中方案。

# IndexFile 与 CommitLog 协作机制详解

## 一、整体架构

RocketMQ 的消息存储采用**顺序写 + 索引读**的设计，CommitLog 负责存储原始消息，IndexFile 提供 Key 级别的快速查找。

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           消息写入流程                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Producer                                                                   │
│     │                                                                       │
│     ▼                                                                       │
│  ┌──────────────┐     顺序写入     ┌──────────────┐                        │
│  │ Broker       │ ───────────────► │ CommitLog    │                        │
│  │              │                  │ (磁盘文件)    │                        │
│  └──────────────┘                  └──────┬───────┘                        │
│                                           │                                 │
│                              ┌────────────┼────────────┐                   │
│                              │            │            │                   │
│                              ▼            ▼            ▼                   │
│                      ┌───────────┐ ┌───────────┐ ┌───────────┐            │
│                      │ConsumeQueue│ │ IndexFile │ │ 其他索引  │            │
│                      │(消费队列)  │ │ (Key索引) │ │          │            │
│                      └───────────┘ └───────────┘ └───────────┘            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           消息查询流程                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Consumer/Admin                                                             │
│     │                                                                       │
│     │  1. 通过 Key 查询索引                                                  │
│     ▼                                                                       │
│  ┌──────────────┐                                                           │
│  │ IndexFile    │ ◄─── 返回 phyOffset (CommitLog 物理偏移)                  │
│  │              │                                                           │
│  └──────┬───────┘                                                           │
│         │                                                                   │
│         │  2. 根据偏移量读取消息                                              │
│         ▼                                                                   │
│  ┌──────────────┐                                                           │
│  │ CommitLog    │ ◄─── 读取消息内容                                          │
│  │              │                                                           │
│  └──────────────┘                                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、协作流程详解

### 2.1 异步分发架构

CommitLog 写入和 Index 构建是**异步**的，通过 `ReputMessageService` 实现：

```java
// DefaultMessageStore.java
class ReputMessageService extends ServiceThread {
    private volatile long reputFromOffset = 0;  // 当前处理位置

    private void doReput() {
        // 1. 从 CommitLog 读取数据
        SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);

        if (result != null) {
            this.reputFromOffset = result.getStartOffset();

            for (int readSize = 0; readSize < result.getSize(); ) {
                // 2. 解析消息
                DispatchRequest dispatchRequest =
                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(
                        result.getByteBuffer(), false, false);

                if (dispatchRequest.isSuccess()) {
                    // 3. 分发到各个索引组件
                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                    this.reputFromOffset += size;
                    readSize += size;
                }
            }
        }
    }
}
```

### 2.2 分发器链

```java
// DefaultMessageStore 构造函数中初始化
this.dispatcherList = new LinkedList<>();
this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());  // 消费队列
this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());         // Key 索引

// doDispatch 方法遍历执行
public void doDispatch(DispatchRequest req) {
    for (CommitLogDispatcher dispatcher : this.dispatcherList) {
        dispatcher.dispatch(req);
    }
}
```

### 2.3 索引构建分发器

```java
// 构建 ConsumeQueue（用于消费拉取）
class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {
    @Override
    public void dispatch(DispatchRequest request) {
        final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
        switch (tranType) {
            case MessageSysFlag.TRANSACTION_NOT_TYPE:
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                // 写入 ConsumeQueue
                DefaultMessageStore.this.putMessagePositionInfo(request);
                break;
            case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                // 准备阶段不写入
                break;
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                // 回滚不写入
                break;
        }
    }
}

// 构建 IndexFile（用于 Key 查询）
class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {
    @Override
    public void dispatch(DispatchRequest request) {
        if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
            DefaultMessageStore.this.indexService.buildIndex(request);
        }
    }
}
```

### 2.4 DispatchRequest 结构

```java
public class DispatchRequest {
    private final String topic;           // 主题
    private final int queueId;            // 队列ID
    private final long commitLogOffset;   // ★ CommitLog 物理偏移
    private int msgSize;                  // 消息大小
    private final long tagsCode;          // Tag 哈希值
    private final long storeTimestamp;    // 存储时间戳
    private final long consumeQueueOffset;// 消费队列偏移
    private final String keys;            // ★ 业务 Key
    private final String uniqKey;         // ★ 唯一标识（客户端生成）
    private final int sysFlag;            // 系统标志（事务类型等）
    // ...
}
```

### 2.5 索引构建流程

```java
// IndexService.buildIndex()
public void buildIndex(DispatchRequest req) {
    IndexFile indexFile = retryGetAndCreateIndexFile();
    if (indexFile != null) {
        long endPhyOffset = indexFile.getEndPhyOffset();

        // 去重检查：如果消息偏移小于索引已记录偏移，说明已处理过
        if (req.getCommitLogOffset() < endPhyOffset) {
            return;
        }

        final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
        switch (tranType) {
            case MessageSysFlag.TRANSACTION_NOT_TYPE:
            case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                break;
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                return;  // 回滚消息不建索引
        }

        // 1. 索引 uniqKey（消息唯一标识）
        if (req.getUniqKey() != null) {
            indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
        }

        // 2. 索引用户指定的 keys（可多个，用空格分隔）
        if (keys != null && keys.length() > 0) {
            String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
            for (int i = 0; i < keyset.length; i++) {
                String key = keyset[i];
                if (key.length() > 0) {
                    indexFile = putKey(indexFile, msg, buildKey(topic, key));
                }
            }
        }
    }
}

// 构建索引键：topic#key
private String buildKey(final String topic, final String key) {
    return topic + "#" + key;
}
```

### 2.6 写入 IndexFile

```java
// IndexFile.putKey()
public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
    if (this.indexHeader.getIndexCount() < this.indexNum) {
        // 1. 计算哈希位置
        int keyHash = indexKeyHashMethod(key);
        int slotPos = keyHash % this.hashSlotNum;
        int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

        // 2. 读取原槽值（链表头）
        int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

        // 3. 计算时间差（秒级精度）
        long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
        timeDiff = timeDiff / 1000;

        // 4. 计算索引存储位置（追加写）
        int absIndexPos = IndexHeader.INDEX_HEADER_SIZE
                        + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

        // 5. 写入索引项
        this.mappedByteBuffer.putInt(absIndexPos, keyHash);              // 哈希值（4B）
        this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);       // CommitLog偏移（8B）
        this.mappedByteBuffer.putInt(absIndexPos + 12, (int) timeDiff);  // 时间差（4B）
        this.mappedByteBuffer.putInt(absIndexPos + 16, slotValue);       // 链表指针（4B）

        // 6. 更新槽指向新索引（头插法）
        this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

        // 7. 更新统计
        this.indexHeader.incIndexCount();
        return true;
    }
    return false;  // 文件已满，需要创建新文件
}
```

---

## 三、消息查询流程

### 3.1 从 Key 到消息

```
┌─────────────────────────────────────────────────────────────────┐
│                     消息查询完整流程                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. 构建查询 Key                                                 │
│     String idxKey = topic + "#" + key;                          │
│     如："OrderTopic#ORDER_20240311001"                          │
│         │                                                       │
│         ▼                                                       │
│  2. 计算哈希定位槽                                               │
│     int keyHash = Math.abs(idxKey.hashCode());                  │
│     int slotPos = keyHash % 5000000;                            │
│         │                                                       │
│         ▼                                                       │
│  3. 读取哈希槽获取链表头                                          │
│     HashSlot[slotPos] = 1523  ──→  指向 index[1523]             │
│         │                                                       │
│         ▼                                                       │
│  4. 遍历链表匹配                                                  │
│     index[1523]: keyHash=xxx, phyOffset=8388608, prevIndex=1201 │
│         │                                                       │
│         ▼ (keyHash匹配，时间范围匹配)                             │
│     记录 phyOffset = 8388608                                    │
│         │                                                       │
│         ▼                                                       │
│     index[1201]: keyHash=xxx, phyOffset=4194304, prevIndex=0    │
│         │                                                       │
│         ▼ (链表结束)                                             │
│     prevIndex = 0，停止遍历                                      │
│         │                                                       │
│         ▼                                                       │
│  5. 根据 phyOffset 从 CommitLog 读取消息                          │
│     CommitLog.read(phyOffset=8388608)                           │
│         │                                                       │
│         ▼                                                       │
│     返回消息内容                                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 查询入口

```java
// IndexService.queryOffset()
public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
    List<Long> phyOffsets = new ArrayList<Long>(maxNum);

    try {
        this.readWriteLock.readLock().lock();

        // 从最新的索引文件开始查找
        for (int i = this.indexFileList.size(); i > 0; i--) {
            IndexFile f = this.indexFileList.get(i - 1);

            // 检查时间范围是否匹配
            if (f.isTimeMatched(begin, end)) {
                // 在此文件中查找
                f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
            }

            // 优化：如果文件起始时间小于查询开始时间，停止查找
            if (f.getBeginTimestamp() < begin) {
                break;
            }

            if (phyOffsets.size() >= maxNum) {
                break;
            }
        }
    } finally {
        this.readWriteLock.readLock().unlock();
    }

    return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
}
```

### 3.3 从 CommitLog 读取消息

```java
// 根据 phyOffset 读取消息
public MessageExt lookMessageByOffset(long commitLogOffset) {
    SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
    if (null != sbr) {
        try {
            // 1. 先读取消息大小
            ByteBuffer bb = sbr.getByteBuffer();
            int size = bb.getInt();

            // 2. 再读取完整消息
            return this.lookMessageByOffset(commitLogOffset, size);
        } finally {
            sbr.release();
        }
    }
    return null;
}
```

---

## 四、与 HashMap 的对比分析

### 4.1 整体对比

| 特性 | RocketMQ IndexFile | Java HashMap |
|------|-------------------|--------------|
| **存储介质** | 磁盘（内存映射） | 堆内存 |
| **数据结构** | 哈希表 + 链表 | 哈希表 + 链表/红黑树 |
| **冲突解决** | 链地址法 | 链地址法（JDK8+ 链表转红黑树）|
| **插入方式** | 头插法（新节点在头部） | 尾插法（JDK8+） |
| **持久化** | 自动持久化 | 内存中，不持久化 |
| **容量限制** | 固定大小（约420MB/文件） | 动态扩容（2倍扩容） |
| **时间范围查询** | 支持 | 不支持 |
| **存储内容** | phyOffset（8字节地址） | 实际对象引用 |

### 4.2 数据结构对比

```
┌────────────────────────────────────────────────────────────────────┐
│                         HashMap (JDK8)                             │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  Node[] table;                                                     │
│  ├── table[0] → null                                               │
│  ├── table[1] → Node(key="a", hash=1, value=obj, next=null)       │
│  ├── table[2] → Node(key="b", hash=2, value=obj, next=...)        │
│  │                     ↓                                           │
│  │                   Node(key="c", hash=2, value=obj, next=null)  │
│  └── ...                                                           │
│                                                                    │
│  特点：                                                            │
│  • 数组容量动态扩容（默认16，2倍增长）                               │
│  • 链表长度>8且数组长度>64时转红黑树                                 │
│  • 存储的是 key→value 的完整映射                                     │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│                      RocketMQ IndexFile                            │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  MappedByteBuffer:                                                 │
│  ┌─────────────┬──────────────┬─────────────────────────────────┐ │
│  │ Header 40B  │ Slots 20MB   │ Index Items 400MB               │ │
│  ├─────────────┼──────────────┼─────────────────────────────────┤ │
│  │ beginTime   │ slot[0]=0    │ index[0]: hash, phyOffset,      │ │
│  │ endTime     │ slot[1]=3    │          timeDiff, prevIndex    │ │
│  │ beginOffset │ slot[2]=0    │ index[1]: hash, phyOffset,      │ │
│  │ endOffset   │ slot[3]=5    │          timeDiff, prevIndex    │ │
│  │ slotCount   │ slot[4]=0    │ index[2]: ...                   │ │
│  │ indexCount  │ slot[5]=1    │ index[3]: ...                   │ │
│  └─────────────┴──────────────┴─────────────────────────────────┘ │
│                                                                    │
│  slot[1]=3 → index[3] → index[1] → index[0]  （倒序链表）           │
│                ↓          ↓          ↓                            │
│              prev=1    prev=0    prev=0(invalid)                  │
│                                                                    │
│  特点：                                                            │
│  • 文件大小固定（Header + 500万slots + 2000万indexes）              │
│  • 写满后创建新文件（时间命名）                                      │
│  • 链表不会转树（保持简单，依赖哈希分布）                              │
│  • 存储的是 topic#key → phyOffset 的映射                            │
│  • 有时间范围过滤能力                                               │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### 4.3 核心代码对比

#### 哈希定位

```java
// HashMap
static final int hash(Object key) {
    int h;
    return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
}

int n = table.length;
int index = (n - 1) & hash;  // 等价于 hash % n

// IndexFile
int keyHash = Math.abs(key.hashCode());
int slotPos = keyHash % this.hashSlotNum;  // hashSlotNum = 500万
int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * 4;
```

#### 冲突处理 - 链表插入

```java
// HashMap - 尾插法（JDK8）
for (int binCount = 0; ; ++binCount) {
    if ((e = p.next) == null) {
        p.next = newNode(hash, key, value, null);  // 插到尾部
        if (binCount >= TREEIFY_THRESHOLD - 1)  // 超过8个转红黑树
            treeifyBin(tab, hash);
        break;
    }
    if (e.hash == hash && ((k = e.key) == key || key.equals(k)))
        break;
    p = e;
}

// IndexFile - 头插法
int slotValue = this.mappedByteBuffer.getInt(absSlotPos);  // 读取原链表头

// 新索引项的 prevIndex 指向原链表头
this.mappedByteBuffer.putInt(absIndexPos + 16, slotValue);

// 哈希槽指向新索引项（新节点成为链表头）
this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
```

### 4.4 设计差异原因

| 差异点 | 原因分析 |
|--------|----------|
| **IndexFile 用头插法** | 新消息查询概率更高，头插法让新数据更快被访问；文件追加写更自然 |
| **IndexFile 不转红黑树** | 磁盘文件结构复杂，保持简单链表；哈希槽够多（500万）时冲突可控 |
| **IndexFile 固定大小** | 便于内存映射管理；文件滚动简化过期清理 |
| **IndexFile 存偏移量** | 解耦索引和消息存储；索引文件小，可常驻内存映射 |
| **IndexFile 有时间字段** | 消息系统常见查询模式（按时间范围查）；利用时间差压缩存储 |

---

## 五、关键协作关系图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              组件协作关系图                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   CommitLog                    ReputMessageService                              │
│  ┌──────────────┐              ┌──────────────────┐                             │
│  │              │              │                  │                             │
│  │ MessageQueue │◄─────────────┤ checkMessageAnd  │                             │
│  │  (文件队列)   │   顺序读取    │ ReturnSize()     │                             │
│  │              │              │                  │                             │
│  └──────┬───────┘              └────────┬─────────┘                             │
│         │                               │                                       │
│         │ 写入消息                      │ DispatchRequest                       │
│         │                               │                                       │
│         ▼                               ▼                                       │
│  ┌──────────────┐              ┌──────────────────┐                             │
│  │              │              │   doDispatch()   │                             │
│  │   磁盘文件    │              └────────┬─────────┘                             │
│  │              │                       │                                       │
│  └──────────────┘              ┌────────┴────────┐                              │
│                                │                 │                              │
│                                ▼                 ▼                              │
│                       ┌──────────────┐   ┌──────────────┐                       │
│                       │ ConsumeQueue │   │ IndexService │                       │
│                       │ Dispatcher   │   │              │                       │
│                       └──────┬───────┘   └──────┬───────┘                       │
│                              │                  │                               │
│                              ▼                  ▼                               │
│                       ┌──────────────┐   ┌──────────────┐                       │
│                       │ ConsumeQueue │   │  IndexFile   │                       │
│                       │ (topic/queue)│   │ (topic#key)  │                       │
│                       └──────────────┘   └──────────────┘                       │
│                                                                                 │
│   ═══════════════════════════════════════════════════════════════════          │
│                                                                                 │
│   查询时：                                                                       │
│                                                                                 │
│   Consumer ──► IndexService.queryOffset() ──► IndexFile                         │
│                                                      │                          │
│                                                      ▼                          │
│   Consumer ◄── CommitLog.read(phyOffset) ◄─── 返回 phyOffset                    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 六、源码文件位置

| 文件 | 路径 | 说明 |
|------|------|------|
| DefaultMessageStore | `store/src/main/java/org/apache/rocketmq/store/DefaultMessageStore.java` | 存储引擎主类，包含 ReputMessageService 和 Dispatcher |
| CommitLog | `store/src/main/java/org/apache/rocketmq/store/CommitLog.java` | CommitLog 实现，消息写入入口 |
| CommitLogDispatcher | `store/src/main/java/org/apache/rocketmq/store/CommitLogDispatcher.java` | 分发器接口 |
| DispatchRequest | `store/src/main/java/org/apache/rocketmq/store/DispatchRequest.java` | 分发请求封装 |
| IndexService | `store/src/main/java/org/apache/rocketmq/store/index/IndexService.java` | 索引服务，协调多个 IndexFile |
| IndexFile | `store/src/main/java/org/apache/rocketmq/store/index/IndexFile.java` | 单个索引文件实现 |
| IndexHeader | `store/src/main/java/org/apache/rocketmq/store/index/IndexHeader.java` | 索引文件头信息 |

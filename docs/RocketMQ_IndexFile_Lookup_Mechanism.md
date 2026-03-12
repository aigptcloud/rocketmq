# RocketMQ 索引文件查找机制详解

## 概述

RocketMQ 为了支持通过消息 Key 或唯一标识（uniqKey）快速查找消息，引入了**索引文件（Index File）**机制。这是一种基于**哈希表 + 链表**的数据结构，能够在大量消息中高效定位特定消息。

---

## 一、索引文件结构

每个索引文件由三部分组成，总大小固定：

```
┌─────────────────────────────────────────────────────────────────┐
│                         IndexFile 结构                           │
├─────────────────────────────────────────────────────────────────┤
│  Index Header (40 bytes)                                        │
│  ├── beginTimestamp (8 bytes)    文件起始时间戳                  │
│  ├── endTimestamp   (8 bytes)    文件结束时间戳                  │
│  ├── beginPhyOffset (8 bytes)    起始物理偏移量(CommitLog)       │
│  ├── endPhyOffset   (8 bytes)    结束物理偏移量(CommitLog)       │
│  ├── hashSlotCount  (4 bytes)    已使用哈希槽数量                │
│  └── indexCount     (4 bytes)    已写入索引项数量                │
├─────────────────────────────────────────────────────────────────┤
│  Hash Slot Table (500万个槽 * 4 bytes = 约20MB)                  │
│  ├── slot[0] (4 bytes)  存储索引链表头指针                       │
│  ├── slot[1] (4 bytes)                                          │
│  └── ...                                                         │
├─────────────────────────────────────────────────────────────────┤
│  Index Linked List (2000万个索引 * 20 bytes = 约400MB)           │
│  ├── index[0] (20 bytes)                                        │
│  │   ├── keyHash    (4 bytes)   Key的哈希值                      │
│  │   ├── phyOffset  (8 bytes)   CommitLog物理偏移量              │
│  │   ├── timeDiff   (4 bytes)   与文件起始时间的时间差(秒)       │
│  │   └── prevIndex  (4 bytes)   上一个冲突索引的位置             │
│  ├── index[1] (20 bytes)                                        │
│  └── ...                                                         │
└─────────────────────────────────────────────────────────────────┘
```

### 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `maxHashSlotNum` | 5,000,000 | 哈希槽数量 |
| `maxIndexNum` | 20,000,000 | 最大索引项数量 |
| 单个文件大小 | 约420MB | Header + Slots + Index |

---

## 二、索引构建流程

当消息写入 CommitLog 后，`IndexService` 会异步构建索引：

### 2.1 构建索引键

```java
// topic + "#" + key 或 topic + "#" + uniqKey
String idxKey = topic + "#" + key;
```

### 2.2 计算哈希位置

```java
// 1. 计算 Key 的哈希值
int keyHash = Math.abs(key.hashCode());

// 2. 定位到哈希槽位置
int slotPos = keyHash % hashSlotNum;  // 默认 500万
int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * 4; // 40 + slotPos * 4
```

### 2.3 写入索引项

```java
// 1. 获取当前哈希槽存储的值（上一个冲突索引的位置）
int slotValue = mappedByteBuffer.getInt(absSlotPos);

// 2. 计算时间差（秒级精度）
long timeDiff = (storeTimestamp - beginTimestamp) / 1000;

// 3. 计算索引项存储位置
int absIndexPos = IndexHeader.INDEX_HEADER_SIZE
                + hashSlotNum * 4           // Hash Slot 区域大小
                + indexCount * 20;          // 当前索引项偏移

// 4. 写入索引项 (20 bytes)
mappedByteBuffer.putInt(absIndexPos, keyHash);        // 哈希值
mappedByteBuffer.putLong(absIndexPos + 4, phyOffset); // CommitLog 偏移
mappedByteBuffer.putInt(absIndexPos + 12, timeDiff);  // 时间差
mappedByteBuffer.putInt(absIndexPos + 16, slotValue); // 上一个冲突索引

// 5. 更新哈希槽，指向最新索引项
mappedByteBuffer.putInt(absSlotPos, indexCount);

// 6. 更新文件头
indexHeader.incIndexCount();
```

### 2.4 哈希冲突处理

RocketMQ 使用**链地址法（Separate Chaining）**解决哈希冲突：

```
哈希槽[slotPos] = 3  ──→  index[3]  ──→  index[1]  ──→  index[0]
                              ↓              ↓              ↓
                           prevIndex=1    prevIndex=0   prevIndex=0 (无效)
```

- 新索引项的 `prevIndex` 指向槽中原来的索引
- 哈希槽始终指向最新的索引项
- 形成倒序链表，最新数据在前

---

## 三、索引查找流程

### 3.1 入口方法

```java
// IndexService.java
public QueryOffsetResult queryOffset(String topic, String key,
                                     int maxNum, long begin, long end)
```

### 3.2 查找步骤

```java
// 1. 构建查询 Key
String idxKey = topic + "#" + key;

// 2. 计算哈希值和槽位置
int keyHash = Math.abs(idxKey.hashCode());
int slotPos = keyHash % hashSlotNum;
int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * 4;

// 3. 读取槽中的链表头
int slotValue = mappedByteBuffer.getInt(absSlotPos);

// 4. 遍历链表查找匹配项
for (int nextIndex = slotValue; ; ) {
    // 计算索引项位置
    int absIndexPos = IndexHeader.INDEX_HEADER_SIZE
                    + hashSlotNum * 4
                    + nextIndex * 20;

    // 读取索引项
    int keyHashRead = mappedByteBuffer.getInt(absIndexPos);
    long phyOffsetRead = mappedByteBuffer.getLong(absIndexPos + 4);
    int timeDiff = mappedByteBuffer.getInt(absIndexPos + 12);
    int prevIndex = mappedByteBuffer.getInt(absIndexPos + 16);

    // 计算实际时间戳
    long timeRead = beginTimestamp + timeDiff * 1000;

    // 匹配条件：哈希值相同 + 时间范围匹配
    if (keyHash == keyHashRead && timeRead >= begin && timeRead <= end) {
        phyOffsets.add(phyOffsetRead);  // 找到匹配
    }

    // 继续遍历链表
    if (prevIndex <= 0 || prevIndex == nextIndex) break;
    nextIndex = prevIndex;
}
```

### 3.3 多文件查找

索引文件按时间滚动，查找时需要遍历多个文件：

```java
// 从最新的文件开始查找
for (int i = indexFileList.size(); i > 0; i--) {
    IndexFile f = indexFileList.get(i - 1);

    // 时间范围匹配检查
    if (f.isTimeMatched(begin, end)) {
        f.selectPhyOffset(phyOffsets, key, maxNum, begin, end, lastFile);
    }

    // 如果文件起始时间小于查询开始时间，停止查找
    if (f.getBeginTimestamp() < begin) break;

    if (phyOffsets.size() >= maxNum) break;
}
```

---

## 四、核心数据结构

### 4.1 IndexHeader（文件头）

```java
public class IndexHeader {
    public static final int INDEX_HEADER_SIZE = 40;

    private AtomicLong beginTimestamp;   // 索引文件第一个消息时间
    private AtomicLong endTimestamp;     // 索引文件最后一个消息时间
    private AtomicLong beginPhyOffset;   // 第一个消息在CommitLog的偏移
    private AtomicLong endPhyOffset;     // 最后一个消息在CommitLog的偏移
    private AtomicInteger hashSlotCount; // 已使用哈希槽数
    private AtomicInteger indexCount;    // 已写入索引项数（从1开始）
}
```

### 4.2 索引项结构

| 字段 | 类型 | 大小 | 说明 |
|------|------|------|------|
| keyHash | int | 4 bytes | Key的哈希值（用于快速比较） |
| phyOffset | long | 8 bytes | 消息在CommitLog的物理偏移量 |
| timeDiff | int | 4 bytes | 与文件起始时间的时间差（秒） |
| prevIndex | int | 4 bytes | 链表指针，指向上一个冲突索引 |

---

## 五、关键源码分析

### 5.1 写入索引 - IndexFile.putKey()

```java
public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
    if (this.indexHeader.getIndexCount() < this.indexNum) {
        // 1. 计算哈希和槽位置
        int keyHash = indexKeyHashMethod(key);
        int slotPos = keyHash % this.hashSlotNum;
        int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

        // 2. 读取原槽值（链表头）
        int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

        // 3. 计算时间差（秒级）
        long timeDiff = (storeTimestamp - this.indexHeader.getBeginTimestamp()) / 1000;

        // 4. 计算索引存储位置
        int absIndexPos = IndexHeader.INDEX_HEADER_SIZE
                        + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

        // 5. 写入索引项
        this.mappedByteBuffer.putInt(absIndexPos, keyHash);
        this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
        this.mappedByteBuffer.putInt(absIndexPos + 12, (int) timeDiff);
        this.mappedByteBuffer.putInt(absIndexPos + 16, slotValue); // 链表指针

        // 6. 更新槽指向新索引
        this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

        // 7. 更新统计
        this.indexHeader.incIndexCount();
        return true;
    }
    return false; // 文件已满
}
```

### 5.2 查询索引 - IndexFile.selectPhyOffset()

```java
public void selectPhyOffset(final List<Long> phyOffsets, final String key,
                            final int maxNum, final long begin, final long end, boolean lock) {
    if (this.mappedFile.hold()) {
        // 1. 定位哈希槽
        int keyHash = indexKeyHashMethod(key);
        int slotPos = keyHash % this.hashSlotNum;
        int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

        // 2. 获取链表头
        int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

        if (slotValue > invalidIndex && slotValue <= this.indexHeader.getIndexCount()) {
            // 3. 遍历链表
            for (int nextIndexToRead = slotValue; ; ) {
                if (phyOffsets.size() >= maxNum) break;

                // 4. 读取索引项
                int absIndexPos = IndexHeader.INDEX_HEADER_SIZE
                                + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 12);
                int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 16);

                // 5. 还原时间戳
                long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff * 1000;

                // 6. 匹配检查
                boolean timeMatched = (timeRead >= begin) && (timeRead <= end);
                if (keyHash == keyHashRead && timeMatched) {
                    phyOffsets.add(phyOffsetRead);
                }

                // 7. 链表结束条件
                if (prevIndexRead <= invalidIndex || prevIndexRead == nextIndexToRead) break;
                nextIndexToRead = prevIndexRead;
            }
        }
        this.mappedFile.release();
    }
}
```

---

## 六、文件管理

### 6.1 文件创建

```java
public IndexFile getAndCreateLastIndexFile() {
    // 检查当前文件是否已满
    if (currentFile.isWriteFull()) {
        // 创建新文件，以当前时间命名
        String fileName = storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
        IndexFile newFile = new IndexFile(fileName, hashSlotNum, indexNum,
                                          lastEndPhyOffset, lastEndTimestamp);
        indexFileList.add(newFile);

        // 异步刷盘旧文件
        flushAsync(prevFile);
    }
}
```

### 6.2 文件过期删除

```java
public void deleteExpiredFile(long offset) {
    // 当 CommitLog 被清理时，同步清理对应的索引文件
    // 判断条件：文件的 endPhyOffset < CommitLog 最小偏移量
    if (indexFile.getEndPhyOffset() < offset) {
        file.destroy();
        indexFileList.remove(file);
    }
}
```

---

## 七、性能特点

| 特性 | 说明 |
|------|------|
| **时间复杂度** | O(1) 哈希定位 + O(k) 链表遍历（k为冲突数） |
| **空间效率** | 每个索引项仅20字节，支持2000万索引/文件 |
| **时间范围过滤** | 索引项存储相对时间差，支持按时间范围查询 |
| **内存映射** | 使用 MappedByteBuffer，实现零拷贝访问 |
| **线程安全** | ReadWriteLock 保证并发读写安全 |

---

## 八、配置参数

```properties
# broker.conf
# 单个索引文件哈希槽数量（默认500万）
maxHashSlotNum=5000000

# 单个索引文件最大索引项数（默认2000万）
maxIndexNum=20000000

# 每次查询返回最大消息数
maxMsgsNumBatch=64
```

---

## 九、使用场景

1. **按 Key 查询消息**：`MQAdmin#queryMessage(topic, key, maxNum, begin, end)`
2. **消息轨迹追踪**：通过 uniqKey 查询消息发送记录
3. **消息审计**：按业务 Key 检索特定消息

---

## 十、源码文件位置

| 文件 | 路径 |
|------|------|
| IndexFile | `store/src/main/java/org/apache/rocketmq/store/index/IndexFile.java` |
| IndexHeader | `store/src/main/java/org/apache/rocketmq/store/index/IndexHeader.java` |
| IndexService | `store/src/main/java/org/apache/rocketmq/store/index/IndexService.java` |
| QueryOffsetResult | `store/src/main/java/org/apache/rocketmq/store/index/QueryOffsetResult.java` |

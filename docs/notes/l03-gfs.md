# GFS

## 系统架构

- **GFS client**：维护专用接口，与应用交互。
- **GFS master**：维护元数据，统一管理chunk位置与租约。
- **GFS chunkserver**：存储数据。

![GFS_Architecture](https://s2.loli.net/2023/12/07/4LcEKqD8jkZFRlP.png)

## 存储设计

*A1. 文件怎样分散存储在多台服务器上？*

> 分块存储。

*B1. 怎样支持大文件（几GB）存储？*

> 采用更大的chunk，以及配套的一致性策略。

- 考虑到文件可能非常大，且大小不均。GFS没有直接以文件为单位进行存储，而是把文件分成一个个chunk（64MB）来存储。
- 相对而言，64MB这个值是偏大的。为什么这样设计？
  + GFS存储的文件都偏大（几GB），较大的chunk可以减少内部的寻址和交互次数；
  + client可能在一个chunk上执行多次操作，这可以复用TCP连接，减少网络开销；
  + 可减少chunk数量，节省元数据，相当于节省了内存，这对GFS很关键。
- 当然，更大的chunk意味着多线程操作同一个chunk的可能性增加，容易产生热点问题，GFS在一致性设计方面做出了对于性能的妥协。

### chunk在chunkserver中的分布

![files_in_chunkservers.png](https://s2.loli.net/2023/12/07/OBQx1NkCJFK9hU7.png)

## GFS Master

*A1: 怎样实现自动扩容？*

> 在master单点上增减、调整chunk的元数据。

*A2: 怎样知道一个文件存储在哪台机器上？*

> 根据master中文件到chunk再到chunk位置的映射来定位具体的chunkserver。

文件位置等信息 -> **元数据**

管理元数据的节点设计为一个单点还是（分布式的）多节点？

- 单节点
  + 实现难度低，容易保证一致性
  + 单点可能会成为系统瓶颈
  + 工作方案重心：缩减元数据，减少单master的压力
- 多节点
  + 实现难度极高，一致性难保证，系统可靠性难验证
  + 不存在瓶颈，可扩展性极强
  + 工作方案重心：设计一个分布式的元数据管理功能，并验证其可靠性


GFS选择的是单master节点，用于存储三类元数据：

1. 所有文件和chunk的**namespace**（文件名，chunk ID）【持久化】
2. **文件到chunk的映射**【持久化】
3. **每个chunk的位置**【不持久化】

GFS读文件的大体流程：文件名 -> 获取文件对应的所有chunk ID -> 获取所有chunk的位置 -> 依次从对应chunkserver中读取chunk

- 为什么chunk的位置不做持久化？
- 因为master在重启时，可以从各个chunkserver收集chunk的位置信息。而文件名、chunk ID以及文件到chunk的映射是master独有的，无法从chunkserver收集得到，因此需要持久化。

GFS采用了一系列措施来确保master不会成为整个系统的瓶颈：
- GFS所有的数据流都不经过master，而是直接由client和chunkserver交互；
  + **分离数据流和控制流**，只有控制流才经过master
- GFS client会**缓存**master中的元数据，在大部分情况下都无需访问master；
- 减少master的**内存**（eg. 增大chunk大小、定制化压缩元数据，等）。

一个64MB的chunk，其元数据小于64字节。假设一个文件存三份，存1PB的数据只有3GB的元数据。
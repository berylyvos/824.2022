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

*A1. 怎样实现自动扩容？*

> 在master单点上增减、调整chunk的元数据。

*A2. 怎样知道一个文件存储在哪台机器上？*

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

为什么chunk的位置不做持久化？
- 因为master在重启时，可以从各个chunkserver收集chunk的位置信息。而文件名、chunk ID以及文件到chunk的映射是master独有的，无法从chunkserver收集得到，因此需要持久化。

GFS采用了一系列措施来确保master不会成为整个系统的瓶颈：
- GFS所有的数据流都不经过master，而是直接由client和chunkserver交互；
  + **分离数据流和控制流**，只有控制流才经过master
- GFS client会**缓存**master中的元数据，在大部分情况下都无需访问master；
- 减少master的**内存**（eg. 增大chunk大小、定制化压缩元数据，等）。

一个64MB的chunk，其元数据小于64字节。假设一个文件存三份，存1PB的数据只有3GB的元数据。

## 高可用设计

*A3. 怎样保证服务器在故障时文件不损坏不丢失？*

> master的WAL和主备、chunk的多副本。

*B2. 超多机器的情况下，怎样实现自动监控、容错与恢复？*

> master的主备切换由chubby负责，chunk的租约、副本位置和数量由master负责。

高可用问题现在比较通用的做法**是共识算法**，如Paxos和Raft。而GFS当时的共识算法还不太成熟，因此GFS借鉴了**主备**的思想，为系统的**元数据**和**文件数据**都单独设计了高可用方案。

因为GFS的机器数量很多，故个别机器宕机十分频繁。面对节点宕机，GFS能**自动切换主备**。

### 元数据

master的三类元数据中，文件名、chunk ID以及文件与chunk的映射，因为是master独有的，因此是必须要持久化的，也自然要保证其高可用。

GFS正在使用的master称为**primary master**。此外，GFS还维护一个**shadow master**作为备份。

master在正常运行时，对元数据做的所有修改操作，都要先记录日志（**WAL**），然后再真正修改内存中的元数据。primary master会实时向shadow master同步WAL。

#### 如何实现自动切换？

若master宕机，会通过Google的Chubby（本质上是共识算法）来识别并切换到shadow master，这个切换是秒级的。

master的高可用机制和MySQL的主备机制非常像。

### 文件数据

文件是以chunk为单位来存储的，每个chunk都有三个副本。所以，文件数据的高可用是从chunk的维度来保持的。

GFS保证chunk高可用的思路：
- 对一个chunk的每次写入，必须保证在三个副本中的写入都完成，才视作写入完成；
- 一个chunk的所有副本都具有完整的数据；
- 若一个chunkserver宕机，它上面的所有chunk都有另外两个副本保持了该chunk的数据；
- 若这个宕机的副本在一段时间后还未恢复，则master可以在另一个chunkserver重建一个副本，维持chunk的副本数为3（可配置）；
- 维护chunk的校验和，读取时进行校验。若出现不匹配，chunkserver反馈master，master选择其他副本读取，后重建该副本；
- 为减少master的压力，采用**租约（Lease）**机制，将读写权限下放给某一个chunk副本；
- 得到master租约授权的chunk副本为primary，在租约生效时间内（一般60秒），对该chunk的写操作直接由primary负责；
- 租约的主备只决定**控制流**走向，不影响数据流。

#### chunk副本的放置

master发起创建chunk副本的三种情况：新chunk创建、chunk副本复制（re-replication）和负载均衡（rebalancing）。

- **副本复制**：因某些原因，如chunkserver宕机，导致chunk副本数小于预期值后，新增一个chunk副本。
- **负载均衡**：发生在master定期对chunkserver的监测。若发现某个chunkserver负载过高，就会执行负载均衡操作，将chunk副本搬到另外的chunkserver上。
  
这三个操作中，master对副本位置的选择策略是相同的，都要遵循：
- 新副本所在的chunkserver的资源利用率较低；
- 新副本所在的chunkserver最近创建的chunk副本数较少（为防止瞬间增加大量副本，成为热点）；
- chunk的其他副本不能在同一机架（保证机架或机房级别的高可用）

## 读写流程

*B3. 怎样支持快速的顺序读和追加写？*

> 总体上GFS是三写一读的模式。写入采用流水线和分离数据流与控制流的技术来保证性能；追加对一致性的保证更简单和高效，所以写入多采用追加。
> 读取时，所有副本均可读，优先就近读取提升性能。

**读写需求**

- **快速读**取：为了性能可读到落后的版本，但不能是错的。
- **正确改写**：通常不用在意性能。在意性能的改写可转为追加。
- **快速追加**：为了性能可允许一定的异常，但追加的数据不能丢失。

### 写入

- **流水线**
- **数据流与控制流分离**

流水线是指，client会将文件数据发往离自己最近的一个副本，无论它是否是primary。该副本可以一边接收数据，一边向其他副本转发。这样就控制了数据的流向，节省了网络传输代价。

![write_ctrl_data_flow.png](https://s2.loli.net/2023/12/08/LVagmb24HfAczln.png)

- 1,2 client向master询问要写入chunk的租约在哪个chunkserver上（primary replica），以及其他副本（secondary replicas）的位置（通常client中有缓存）；
- 3 client将数据推送到所有的副本上，这一步就是流水线，即写入过程唯一的数据流操作；
- 4 确认所有副本都收到了数据后，client向primary replica发送正式写入的请求。primary replica收到请求后，对这个chunk上所有的操作排序，然后按照顺序执行写入；
  * 这里很关键，primary replica唯一确定写入顺序，保证副本的一致性。
- 5 primary replica将chunk写入顺序同步给secondary replica；
  * 注意，此时primary replica已写入成功。
- 6 所有的secondary replica向primary replica返回写入完成；
- 7 primary replica返回写入结果给client；
  * 所有副本写入成功：client确认写入完成
  * 部分副本写入失败：client认为写入失败，并从第3步重新执行

如果一个写入涉及到多个chunk，client会分为多个写入来执行。

#### 改写与追加

改写可以完全适配上述的写入步骤，重复执行改写也不会产生副本间的不一致。但改写的问题在于一个改写操作可能涉及到多个chunk，若部分chunk成功，部分失败，读到的文件就是不正确的。改写大概率是一个分布式操作，若要保证改写的强一致性，代价（两阶段提交）就要大很多了。

而追加只涉及到文件的最后一个chunk和可能新增的chunk，即使追加失败，client也只会读到过期而不是错误的数据。论文中强调，GFS推荐使用**追加**的方式写入文件，大部分使用GFS的应用的绝大部分写入也都是追加。

### 读取

- client收到读取一个文件的请求后，首先查看自身缓存中有没有此文件的元数据。若无，则请求master或shadow master获取元数据并缓存；
- client根据文件偏移量计算出对应chunk的位置；
- client向最近的chunkserver发送读请求。若发现该chunkserver没有所需chunk，说明缓存失效，就再请求master获取最新的元数据；
- 进行chunk校验和确认，若不通过，则选择其他副本读取；
- client返回读取结果。

## 一致性模型

*A4. 使用多副本时怎样保证副本之间的一致性？*

> - 对一个chunk所有副本的写入顺序都是一致的。
> - 使用chunk的版本号来检测chunk副本是否出现过宕机。失效的副本不会再进行写入，master不会再记录该副本的信息（client刷缓存时同步），GC自动回收失效副本。
> - master定期检查chunk副本的checksum来确认正确性。
> GFS推荐应用更多地使用追加来达到更高的一致性。

在实际应用中，因为有多个client，写入往往是并发的，这会带来副本间不一致的风险。

GFS将文件数据的一致性大体分为：inconsistent，consistent，defined

- **consistent**：文件无论从哪个副本读取，结果都是一样的。
- **defined**：文件发生了修改操作后，读取是一致的，且client还可以看到最新修改内容（在consistent的基础上，保证了与最新写入的一致）
  
![file_region_state_after_mutation.png](https://s2.loli.net/2023/12/08/PJDFiHBeQoOYRKN.png)

- 串行改写成功：defined。因为所有副本都完成改写后才能返回成功，且重复执行不会产生副本间不一致。
- 并发改写成功：consistent but undefined。并发改写操作可能会涉及到多个chunk，而不同chunk对改写的执行顺序不一定相同，进而导致应用读取不到预期的结果。
- 写入失败：inconsistent。这通常发生在某个副本的chunkserver宕机时，无法在所有副本写入成功。
- 追加写成功：defined interspersed with inconsistent（已定义但可能存在副本间的不一致）。追加的重读执行会造成副本间不一致。

GFS为实现追加的一致性，对追加做了约束：

- **单次追加的大小不超过64MB**（chunk的大小）
- 如果文件最后一个chunk不足以提供追加所需空间，则进行padding，然后新增chunk进行追加。
- 这样，每次追加都会限制在一个chunk上，从而保证追加的原子性。

**重复追加**的问题相对好解决，比如原文件为'ABC'，追加'DEF'。有的副本第一次失败再重复执行就是'ABCDEF'，而两次都正确执行的副本是'ABCDEFDEF'。有很多手段可以确保在读取时只读到一个'DEF'，比如记录文件长度、各副本定期校验。
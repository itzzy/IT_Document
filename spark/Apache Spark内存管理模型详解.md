## Apache Spark内存管理模型详解
---
### 介绍
本文将对 Spark 的内存管理模型进行分析，下面的分析分别基于Spark1.5静态内存管理和Spark 2.2.1统一的内存管理进行。为了让下面的文章看起来不枯燥，我打算贴出代码层面的东西。
我们都知道 Spark 能够有效的利用内存并进行分布式计算，其内存管理模块在整个系统中扮演着非常重要的角色。为了更好地利用 Spark，深入地理解其内存管理模型具有非常重要的意义，这有助于我们对 Spark 进行更好的调优；在出现各种内存问题时，能够摸清头脑，找到哪块内存区域出现问题。在Spark 1.5和之前版本里，两者是静态配置的，不支持借用，spark1.6 对内存管理模块进行了优化，通过内存空间的融合，消除以上限制，提供更好的性能。下文介绍的内存模型全部指 Executor 端的内存模型， Driver 端的内存模型本文不做介绍。统一内存管理模块包括了堆内内存(On-heap Memory)和堆外内存(Off-heap Memory)两大区域，下面对这两块区域进行详细的说明。

### 基本知识
- on-heap memory：Java中分配的非空对象都是由Java虚拟机的垃圾收集器管理的，也称为堆内内存。虚拟机会定期对垃圾内存进行回收，在某些特定的时间点，它会进行一次彻底的回收（full gc）。彻底回收时，垃圾收集器会对所有分配的堆内内存进行完整的扫描，这意味着一个重要的事实——这样一次垃圾收集对Java应用造成的影响，跟堆的大小是成正比的。过大的堆会影响Java应用的性能
- off-heap memory：堆外内存意味着把内存对象分配在Java虚拟机的堆以外的内存，这些内存直接受操作系统管理（而不是虚拟机）。这样做的结果就是能保持一个较小的堆，以减少垃圾收集对应用的影响
- LRU Cache（Least Recently Used）：LRU可以说是一种算法，也可以算是一种原则，用来判断如何从Cache中清除对象，而LRU就是“近期最少使用”原则，当Cache溢出时，最近最少使用的对象将被从Cache中清除
- spark 源码： https://github.com/apache/spark/releases
- scale ide for Intellij : http://plugins.jetbrains.com/plugin/?id=1347

### Spark1.5-内存管理
#### Spark1.5内存管理
- 1.6 版本引入了新的内存管理方案，配置参数： spark.memory.useLegacyMode 默认 false 表示使用新方案，true 表示使用旧方案， SparkEnv.scala 源码 如下图：
	    
- 在staticMemoryManager.scala 类中查看构造类及内存获取定义	         
 
- 通过代码推断，若设置了 spark.testing.memory 则以该配置的值作为 systemMaxMemory，否则使用 JVM 最大内存作为 systemMaxMemory。

- spark.testing.memory 仅用于测试，一般不设置，所以这里我们认为 systemMaxMemory 的值就是 executor 的最大可用内存
- Execution：用于缓存shuffle、join、sort和aggregation的临时数据，通过spark.shuffle.memoryFraction配置
- spark.shuffle.memoryFraction：shuffle 期间占 executor 运行时内存的百分比，用小数表示。在任何时候，用于 shuffle 的内存总 size 不得超过这个限制，超出部分会 spill 到磁盘。如果经常 spill，考虑调大参数值
- spark.shuffle.safetyFraction：为防止 OOM，不能把 systemMaxMemory * spark.shuffle.memoryFraction 全用了，需要有个安全百分比
- 最终用于 execution 的内存量为：executor 最大可用内存* spark.shuffle.memoryFraction*spark.shuffle.safetyFraction，默认为 executor 最大可用内存 * 0.16
- execution内存被分配给JVM里的多个task线程。
- task间的execution内存分配是动态的，如果没有其他tasks存在，Spark允许一个task占用所有可用execution内存
 
- storage内存分配分析过程与 Execution 一致，由上面的代码得出，用于storage 的内存量为: executor 最大可用内存 * spark.storage.memoryFraction * spark.storage.safetyFraction，默认为 executor 最大可用内存 * 0.54
- 在 storage 中，有一部分内存是给 unroll 使用的，unroll 即反序列化 block，该部分占比由 spark.storage.unrollFraction 控制，默认为0.2
 
- 通过代码分析，storage 和 execution 总共使用了 80% 的内存，剩余 20% 内存被系统保留了，用来存储运行中产生的对象,该类型内存不可控.

### 小结
- 这种内存管理方式的缺陷，即 execution 和 storage 内存表态分配，即使在一方内存不够用而另一方内存空闲的情况下也不能共享，造成内存浪费，为解决这一问题，spark1.6 启用新的内存管理方案UnifiedMemoryManager
- staticMemoryManager- jvm 堆内存分配图如下
	 
### Spark2.2.1内存管理
#### 堆内内存(On-heap Memory)
默认情况下，Spark 仅仅使用了堆内内存。Executor 端的堆内内存区域大致可以分为以下四大块：
Spark Memory:包括Execution内存和Storage内存，该部分大小为 (JVM Heap Size - Reserved Memory) * spark.memory.fraction,其中的spark.memory.fraction可以是我们配置的(默认0.75)

- Execution 内存：主要用于存放 Shuffle、Join、Sort、Aggregation 等计算过程中的临时数据
- Storage 内存：主要用于存储 spark 的 cache 数据，例如RDD的缓存、unroll数据；
- 用户内存（User Memory）：主要用于存储 RDD 转换操作所需要的数据，例如 RDD 依赖等信息。
- 预留内存（Reserved Memory）：系统预留内存，会用来存储Spark内部对象。
整个 Executor 端堆内内存如果用图来表示的话，可以概括如下：
 

我们对上图进行以下说明：

- systemMemory = Runtime.getRuntime.maxMemory，其实就是通过参数 spark.executor.memory 或 --executor-memory 配置的。
- reservedMemory 在 Spark 2.2.1 中是写死的，其值等于 300MB，这个值是不能修改的（如果在测试环境下，我们可以通过 spark.testing.reservedMemory 参数进行修改）；
- usableMemory = systemMemory - reservedMemory，这个就是 Spark 可用内存。

- 如果spark.memory.fraction配小了，我们的spark task在执行时产生数据时，包括我们在做cache时就很可能出现经常因为这部分内存不足的情况而产生spill到disk的情况，影响效率。采用官方推荐默认配置
- Spark Memory这一块有被分成了两个部分，Execution Memory 和 Storage Memory,这通过spark.memory.storageFraction来配置两块各占的大小(默认0.5，一边一半)，如图：
- Storage Memory主要用来存储我们cache的数据和临时空间序列化时unroll的数据，以及broadcast变量cache级别存储的内容
- Execution Memory则是spark Task执行时使用的内存(比如shuffle时排序就需要大量的内存)
- 为了提高内存利用率，spark针对Storage Memory 和 Execution Memory有如下策略:

    - 1.	一方空闲，一方内存不足情况下，内存不足一方可以向空闲一方借用内存
    - 2.	只有Execution Memory可以强制拿回Storage Memory在Execution Memory空闲时，借用的Execution Memory的部分内存（如果因强制取回，而Storage Memory数据丢失，重新计算即可）
    - 3.	如果Storage Memory只能等待Execution Memory主动释放占用的Storage Memory空闲时的内存。(这里不强制取回，因为如果task执行，数据丢失就会导致task 失败)

#### 堆外内存(Off-heap Memory)
Spark 1.6 开始引入了Off-heap memory(详见SPARK-11389)。这种模式不在 JVM 内申请内存，而是调用 Java 的 unsafe 相关 API 进行诸如 C 语言里面的 malloc() 直接向操作系统申请内存，由于这种方式不进过 JVM 内存管理，所以可以避免频繁的 GC，这种内存申请的缺点是必须自己编写内存申请和释放的逻辑。
默认情况下，堆外内存是关闭的，我们可以通过 spark.memory.offHeap.enabled 参数启用，并且通过 spark.memory.offHeap.size 设置堆外内存大小，单位为字节。如果堆外内存被启用，那么 Executor 内将同时存在堆内和堆外内存，两者的使用互补影响，这个时候 Executor 中的 Execution 内存是堆内的 Execution 内存和堆外的 Execution 内存之和，同理，Storage 内存也一样。相比堆内内存，堆外内存只区分 Execution 内存和 Storage 内存，其内存分布如下图所示：
 
Execution 内存和 Storage 内存动态调整
细心的同学肯定看到上面两张图中的 Execution 内存和 Storage 内存之间存在一条虚线，这是为什么呢？
用过 Spark 的同学应该知道，在 Spark 1.5 之前，Execution 内存和 Storage 内存分配是静态的，换句话说就是如果 Execution 内存不足，即使 Storage 内存有很大空闲程序也是无法利用到的；反之亦然。这就导致我们很难进行内存的调优工作，我们必须非常清楚地了解 Execution 和 Storage 两块区域的内存分布。而目前 Execution 内存和 Storage 内存可以互相共享的。也就是说，如果 Execution 内存不足，而 Storage 内存有空闲，那么 Execution 可以从 Storage 中申请空间；反之亦然。所以上图中的虚线代表 Execution 内存和 Storage 内存是可以随着运作动态调整的，这样可以有效地利用内存资源。Execution 内存和 Storage 内存之间的动态调整可以概括如下：

具体的实现逻辑如下：
•	程序提交的时候我们都会设定基本的 Execution 内存和 Storage 内存区域（通过 spark.memory.storageFraction参数设置）；
•	在程序运行时，如果双方的空间都不足时，则存储到硬盘；将内存中的块存储到磁盘的策略是按照 LRU 规则进行的。若己方空间不足而对方空余时，可借用对方的空间;（存储空间不足是指不足以放下一个完整的 Block）
•	Execution 内存的空间被对方占用后，可让对方将占用的部分转存到硬盘，然后"归还"借用的空间
•	Storage 内存的空间被对方占用后，目前的实现是无法让对方"归还"，因为需要考虑 Shuffle 过程中的很多因素，实现起来较为复杂；而且 Shuffle 过程产生的文件在后面一定会被使用到，而 Cache 在内存的数据不一定在后面使用。
注意，上面说的借用对方的内存需要借用方和被借用方的内存类型都一样，都是堆内内存或者都是堆外内存，不存在堆内内存不够去借用堆外内存的空间。
Task 之间内存分布
为了更好地使用使用内存，Executor 内运行的 Task 之间共享着 Execution 内存。具体的，Spark 内部维护了一个 HashMap 用于记录每个 Task 占用的内存。当 Task 需要在 Execution 内存区域申请 numBytes 内存，其先判断 HashMap 里面是否维护着这个 Task 的内存使用情况，如果没有，则将这个 Task 内存使用置为0，并且以 TaskId 为 key，内存使用为 value 加入到 HashMap 里面。之后为这个 Task 申请 numBytes 内存，如果 Execution 内存区域正好有大于 numBytes 的空闲内存，则在 HashMap 里面将当前 Task 使用的内存加上 numBytes，然后返回；如果当前 Execution 内存区域无法申请到每个 Task 最小可申请的内存，则当前 Task 被阻塞，直到有其他任务释放了足够的执行内存，该任务才可以被唤醒。每个 Task 可以使用 Execution 内存大小范围为 1/2N ~ 1/N，其中 N 为当前 Executor 内正在运行的 Task 个数。
比如如果 Execution 内存大小为 10GB，当前 Executor 内正在运行的 Task 个数为5，则该 Task 可以申请的内存范围为 10 / (2 * 5) ~ 10 / 5，也就是 1GB ~ 2GB的范围。
一个示例
为了更好的理解上面堆内内存和堆外内存的使用情况，这里给出一个简单的例子。
只用了堆内内存
现在我们提交的 Spark 作业关于内存的配置如下：

--executor-memory 18g
由于没有设置 spark.memory.fraction 和 spark.memory.storageFraction 参数，我们可以看到 Spark UI 关于 Storage Memory 的显示如下：
 

上图很清楚地看到 Storage Memory 的可用内存是 10.1GB，这个数是咋来的呢？根据前面的规则，我们可以得出以下的计算：

 

现在终于对上了。
具体将字节转换成 GB 的计算逻辑如下(core 模块下面的 /core/src/main/resources/org/apache/spark/ui/static/utils.js)：

 

用了堆内和堆外内存
现在如果我们启用了堆外内存，情况咋样呢？我们的内存相关配置如下：
 

参考：
https://mp.weixin.qq.com/s?__biz=MzA5MTc0NTMwNQ==&mid=2650714979&idx=1&sn=a46db858bb837b98d7e1c355a66d97f7&chksm=887dae15bf0a27039ffdb014e2b9178077c7ada6de96e74cc1b3d7ed926bb1cd6dfdf7dbcb0f&scene=21#wechat_redirect
http://www.cnblogs.com/tgzhu/p/5822370.html


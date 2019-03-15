### 文件内容总览
- src/main/java/wordCountDemo/wordCountDemo.scala是使用scala写的wordcount程序
- src/main/java/wordcountJava/wordcount.java是使用java写的wordcount程序
- src/main/java/Api_Join_Demo/joinDemo.scala是演示scala中的join函数
- src/main/java/sparkSQL1/sqlDemo1 使用sparkSQL读取json文件，并打印数据框
- src/main/java/sparkSQL1/sqlDemo1 sparkSQL使用JDBC操纵Mysql数据库,并打印数据框
- src/main/java/sparkStreamingDemo1/sparkStreamingDemo sparkstreaming统计从网络中传来的数据
- src/main/java/sparkStreamingDemo1/JavaSparkStreamingWordCountApp1.java sparkstreaming的窗口化操作
- src/main/java/hiveDemo/hiveDemo1.java 是使用jdbc方式连接到hive数据仓库，数据仓库要开启hiveserver2
- src/main/java/HadoopTextBook 是配套《hadoop权威指南》的
- src/main/java/DeepUnderstandingOfBigData/MatrixMultiply.java 是用mapreduce写的矩阵乘法
### 知识点
1.SparkContext是spark程序的入口点
2.SparkConf是设置spark应用参数的
3关于spark
    3.1spark是MapReduce的扩展，基于内存的运算（mapreduce会将map后的结果存入磁盘，所以
    耗费时间）。
    3.2 spark中的每个partition都会发布到其他worker节点上，worker节点负责计算。
    3.3 如果偏向存储，HBase合适；如果偏向离线统计，Hive合适；如果是实时交互计算，spark合适；
4.关于RDD
  4.1 RDD包含的5个属性（1）分区列表（记录数据都存放在哪里了）（2）针对每个split的计算函数（因为数据量大，所以在
  spark上是在网络间传递函数，而不是传递数据）(3)依赖列表(4)首选位置(5)分区函数
  4.2 变换函数map和mapPartitions，map是对于RDD中的每个元素进行映射转换，而mapPartitions是对
  整个partition进行转换，他们的区别主要在操作对象。
  4.3 要想让两个分区同时进行计算，还需要两个并行线程的支持。
  conf.setMaster("local[n]");中的n指定n个线程
  sc.textFile(path,n)中的n指定n个分区
  一个分区只能在一个线程上跑，但是一个线程可能跑多个分区。
  4.4RDD中存放的其实是一系列变换，是高阶函数
  4.5对数据应用函数而不是对函数应用数据，数据不动，传递函数
  4.6执行计算的时候，RDD会被spark拆分为tasks，tasks是直接供executor计算的
  4.7 在完全分布式运行job之前，spark会进行task的闭包计算，闭包计算就是将每个变量和方法
  发送到各个executor上
5.一些api的讲解
  5.1 join: (k,v).join(k,w) => (k,(v,w))
  5.2 cogroup: (k,v).cogroup(k,w) =>(k,(Iterable<v>,Iterable<w>))(相当于分成了多个以k为key的
  集合)
6.RDD对数据倾斜的处理：
6.1（问题）如果hello world出现了很多次，那么对于(k,v)对map后的（hello,1），所有k为hello
的（k,v）要进入同一个节点，这样尽管有很多节点，也没有并行计算。
6.2（解决方案）针对上面的问题，我们先在每个(hello,1)上的k进行改变，即添加_(0-99)的后缀，
这时候（hello,1)变为（hello_1,1）(hello_2,1)(hello_3,1)...再进行reduceBykey就避免数据倾斜了。
7.关于匿名内部类
   其实就是向函数中传递一个接口对象，可是接口不能定义对象，那就先实现接口作为临时接口，再使用这个临时接口定义一个临时对象
   new PairFunction<String, String, Integer>() {
   //这是PairFunction抽象类中要实现的接口：Tuple2<K, V> call(T t) throws Exception;
    public Tuple2<String, Integer> call(String s) throws Exception {
       return new Tuple2<String, Integer>(s,1);
    }
    拿这段代码来说，new PairFunction<String, String, Integer>()本来就是想定义一个对象，可是PairFunction
    类型是抽象的，有一个call方法要实现，所以它就暂时实现了call方法，再定义一个对象。
8.关于RDD持久化
  8.1持久化就是将分区保存到内存当中，之后在其他操作当中进行重用。
  8.2action第一次计算时会发生持久化，以后的结果直接读取持久化的结果就可以了，如果持久化的
  结果被删除了，那么还可以根据RDD进行重新计算。
  8.3RDD存放的是对数据的变换过程，所以当某个分区的数据丢失的时候，可以根据之前的操作进行恢复。
  8.4持久化是对计算结果的存储，如果结果丢失，那么根据RDD中对数据的变换，那么也可以恢复数据。
  8.5持久化是有级别的，可以选择内存持久化或者硬盘持久化。
9.spark的数据数据共享
  9.1数据共享通过广播变量和累加器两种方式实现
  9.2要成为广播变量的变量必须能够被串行化
  9.3累加器，发送给整个集群，每个节点都可以进行++,类似于点击量
10.sparkSQL可以获取本地文本中的数据作为数据框：src/main/java/sparkSQL1/sqlDemo1
   也可以操作数据库中的数据作为数据框：src/main/java/sparkSQL1/sqlDemo2
11.storm是准实时计算，spark相对于storm，吞吐量比较高，但是处理速度慢一些。
12.关于DStream
  12.1每个批次都是一个RDD，其实DStream就是一个RDD流。
  12.2同一个jvm只能有一个streamingcontext
  12.3窗口化操作举例：计算5分钟之内的热词，但是计算频率是1分钟/次。
  这里的问题是，每次计算一定会有4分钟是上一次算过的数据。
  window的length对应的是窗口长度（上边的5），window的interval对应的是窗口计算间隔（上边的1）。
  12.4几个英文解释：
  batch interval:批次间隔，就是将n秒的数据作为一个RDD
  window length：窗口长度，必须是batch interval的整数倍，也就是计算多长时间内的RDD
  window interval（slide interval）:窗口间隔，必须是batch interval的整数倍，就是触发计算的时间长度，或者说是
  计算的时间频率。
  12.5 updateStateByKey()会在Dstream计算过程中将上次的结果保留，之后累加求和
13.关于窗口化操作
  13.1就是将上边的RDD离散流封装成窗口，具体参数是batch interval,window length,window interval等等
  13.2针对窗口有特定的函数，比如原来的reduceByKey，对应窗口有reduceByKeyAndWindow
  13.3 使用本地socket作为数据源的时候一定要先将nc启动，再启动程序
14.sparkStreaming容错处理
  14.1 接受数据的是执行器1,同时执行器2会备份执行器1中接收过的数据
  14.2 如果executor故障，那么所有未经处理的数据会丢失，解决办法是通过write ahead log（写前日志）
  将数据写入到hdfs中
15.检查点：配置检查点(checkpoint)之后，所有的运算都要备份到检查点中，是一个持久化过程
    设置检查点之后可以让Driver程序从宕机的地方重启，也就是sparkStreaming被中断后，可以从中断的位置重新启动
16.spark集群部署模式
    （四种模式的主要区别在于cluster manager之间的区别）
    1.local
    2.standalone：使用spark进程作为管理节点
    3.mesos：使用mesoso的master作为管理节点
    4.yarn：使用hadoop的ResourceManager作为master节点
17.spark提交job运行模式
    1.client：driver运行在client主机上，client可以不再cluster中。
    2.cluster：driver程序提交给spark cluster的某个worker节点来执行
    worker是cluster的一员
    3.无论哪种运行模式，RDD都在worker节点上运算
18.local和cluster模式下对变量引用的区别
    1.local模式下，所有的executor都是指向一个变量，所以任务对counter的++会得到总的计数值
    2.cluster模式下，运行之前spark先进行闭包处理，每个executor都得到一份变量和方法，所以
    在每个executor中进行counter++只会影响自己的变量，而对其他executor没有影响
18.2 spark中map和flatmap的区别和联系
    map函数会对每一条输入进行指定的操作，然后为每一条输入返回一个对象
    而flatMap函数则是两个操作的集合——正是“先映射后扁平化”：
    操作1：同map函数一样：对每一条输入进行指定的操作，然后为每一条输入返回一个对象
    操作2：最后将所有对象合并为一个对象
19.hive简介
    1.在hadoop上处理结构化数据的数据仓库，采用sql方式操作，数据存储在hdfs上,数据结构(shceme)存储在数据
    库中
    2.不是关系型数据库，不是在线处理，不适合实时查询
    3.启动hive时候要启动hdfs
    4.层次关系hive > mysql > hdfs (自己猜测???)
      hive提交任务，mysql存储数据结构（表结构），最后任务提交给hadoop处理
      hive是客户端，mysql是逻辑层，hadoop是执行工作的
    5.hive中的表
      a)托管表：删除表时，数据也删除
      b)外部表：删除表时，数据不删除
    6.在hive中默认创建托管表
    7.可以通过远程连接，访问hive
       a)先启动hiveserver2服务器，端口是10000
         hive --service hiveserver2 &
         （上边命令相当于启动了hiveservice服务，直接输入hive 相当于只启动了hive cli服务
         也就是命令行）
       b)输入beeline进入beeline命令行
         （beeline也是hive的一个服务，是支持并发访问hiverserver的cli）
       b)通过beeline连接到hiveserver2
         直接beeline启动beeline（连接beeline之后就支持并发访问了）
         连接命令!connect jdbc:hive2://localhost:10000/mydb2
       c)连接之后操作就和mysql一样了
         show databases
    8.一些hive命令：
       a)建表：
          create external table if not exists t2(id int,name string,age int) comment 'xx' row format delimited fields terminated by',' stored as textfile;
       b)将本地文件上传到hive表中（前提是表结构和文本数据一致）
          load data local inpath '/home/zhangjiaqian/hive/testData' into table t2;
          该命令就是将本地文件或者hadoop上的文件移动到hive工作目录下，hive工作目录也是在hadoop上的，
          只不过目录不同。
       c)复制表
         带表结构和数据：create table t4 as select * from t2;
         只有表结构，没有数据：create table t3 like t2;
    9.关于hive查询语句是否转成mr计算（个人理解）
      1) select * from t where age > 3; 
         这条语句不需要转换成mr，因为在每个分区找到age>3再输出就可以了
      2) select count(*) from t;
         这条语句需要转换成mr，因为不做汇集就不能计算出总数
    10.关于hive的分区表
       a) hive的优化手段之一就是建立分区表，分区表是目录，可以限定扫描一定目录下的数据，所以可以减少时间
    比如最近一周的日志，最近一个月的销量，都可以建立单独的分区表
       b) 分区表在表中以字段形式显示，并且在查询的时候可以使用该字段（实际上没有该字段），
       而在存储时候是一个目录，所以表面上是指定查找某个字段的值，实际上是
       在某个目录下查找，缩小了查找范围 
       c)分区表在目录的层次上进行过滤
       d)分区表在逻辑上是添加了两个字段，在物理上是将数据存放在不同目录下
    11.关于hive桶表
       a)桶表的作用就是将同一个表的数据存储到不同文件当中，比如使用id进行hash存储
       b)这样做的好处就是如果按照id查询，那么就可以按照桶，也就是文件进行筛选
         比如查询id=10000的记录，桶表按照1000划分，那么直接查第1个桶（文件）就可以
       c)桶表在文件的层次上进行过滤
       d)直接进行load data local inpath 'xx' into table t;
         不会进行分桶，要使用查询插入，比如insert into t1 select xx from t2;
       e)如果增加一个记录，不会在原来的桶文件中追加内容，而是在原来的文件桶中添加一个copy
         比如：
         原来的是/user/hive/warehouse/mydb1_31.db/employees_bucket/000000_0
         添加一个新数据，如果在一个桶，那么会增加一个
         /user/hive/warehouse/mydb1_31.db/employees_bucket/000000_0_copy_1
       f)桶的大小如何划分：尽量保证一个桶可以装入两个hdfs数据块的数据量
    12.导出表
       a)导出表命令export table t2 to '/home/zhangjiaqian/test/outTable.txt';
       注意这里的输出目录路径不是本地目录，而是在hdfs上的目录
    13.distribute by,sort by,...
        13.1 distribute by的作用就是将每个公司的股票记录都分到每个公司的reduce端中进行排序
        13.2 hive中仍然有group by语句和having语句
        13.3 order by是全局排序，sort by是在每个reduce端进行排序
         两者会在reduce作业个数大于1的时候有区别，此时sort by整体上并没有实现排序
        13.4 cluster by 等于distribute by + sort by 
         cluster by 采用的排序方式也是升序排序
    14.动态分区
       a)严格模式和非严格模式
         严格模式：插入记录或者查询时至少指定一个静态分区
         相反，非严格模式插入记录或者查询时可以不指定静态分区
       b)动态分区：分区表的分区（也就是列）是可以动态增加的
       c)举例
        insert into table employees partition(country,state) 
        select ...,se.cnty,se.st from
        staged employees se where se.cnty = 'us';
        也就是在插入时候不指定分区，分区的具体信息根据记录来确定
        如果所有的都可以动态指定，那就是非严格模式
        如果必须要指定至少一个分区，那就是严格模式
       d)插入记录过程也可以看作是创建文件夹的过程
    15.一句话完成wordcount
       1.原数据
       leetcode is great
       great is leetcode
       is great leetcode
       great leetcode is
       2.select split(line,' ') from wordcount;
       split可以将line这列的每个记录用空格分解成一个数组
       ["leetcode","is","great"]
       ["great","is","leetcode"]
       ["is","great","leetcode"]
       ["great","leetcode","is"]
       3.select explode(split(line,' ')) from wordcount;
       explode可以将多个数组中的元素取出来，变成一列内容
       leetcode
       is
       great
       great
       is
       leetcode
       is
       great
       leetcode
       great
       leetcode
       is
       4.select t.word,count(*) c from ((select explode(split(line,' ')) 
       as word from wordcount)  t) group by t.word order by c desc ;
         分为几部分：
           1.select explode(split(line,' ')将该列定义别名word
           2.(select explode(split(line,' ')) as word from wordcount)  t 
           将该表定义成别名t
           3.这就变成select t.word,count(*) c from t group by t.word order by c desc;
    16.视图
       视图只是对语句的封装，并不存在真正的表（虽然执行过后会有类似表结构的存在）
       在hdfs上没有存放视图，在mysql中存放，说明视图只是逻辑上的，不是物理上的
    17.函数
       17.1 表生成函数就是一行输入，多行输出
            例如select explode(array(1,2,3));
       17.2 自定义函数的@Description中的内容就是在命令行下执行describe function xxx;显示的内容
20.sql笔记
    1.执行顺序 from   where   group by    having    select    order by
    2.select distinct vend_id,prod_price from products;中的结果去除了vend_id和prod_price都一样的行
    也就是说distinct是对所有列作为一个整体去重，而不是单个列
    3.多个条件筛选
      3.1 使用and或者or
          where中包含多个条件时，使用and 或者or 连接，这里and的优先级要高于or的优先级
         （多个条件最好使用括号包含，容易理解）
      3.2 使用in
          in的作用就和or条件差不多，例如
          select prod_name,prod_price from products where vend_id 
          in (1002,1003) order by prod_name;
          就等价于
          select prod_name,prod_price from products where 
          vend_id = 1002 or vend_id = 1003 order by prod_name;
    4.使用通配符进行过滤
      4.1 %代表的是一个或多个字符
      4.2 _代表一个字符
    5.正则表达式
      5.1 regexp匹配列值中是否包含某一个子串
          like匹配列值是否是某一个串
          例如select prod_name from products 
              where prod_name like '_000' 
              order by  prod_name;
          寻找的是字段为“x000”的行，如果某行中有“abc 1000”则不匹配
           而对于regexp '.000',"abc 1000"是匹配的，因为'.000'和'1000'子串匹配
      5.2 .表示一个字符,[]表示范围中的一个,|表示或
          * 表示0个或多个，+表示1个或多个,?表示0个或1个
          {n}表示n个，{n,}表示n个或多余n个，{n,m}表示[n,m]个
          ^表示文本的开始，$表示文本的结束
      5.3 匹配条件带有或
          select prod_name from products 
          where prod_name regexp '1000|2000' 
          order by  prod_name;
      5.4 匹配几个字符之一：[xyz]表示x,y,z之一
          select prod_name from products 
          where prod_name regexp '[123] Ton' 
          order by prod_name;
          匹配范围：[1-5]表示1,2,3,4,5之一
      5.5 查询带有.的值，要转义
          select vend_name from vendors 
          where vend_name regexp '\\.' 
          order by vend_name;
      5.6 匹配包含四位数字的行：
        方法1：select prod_name from products 
        where prod_name regexp '[[:digit:]][[:digit:]][[:digit:]][[:digit:]]' 
        order by prod_name;
        方法2：select prod_name from products 
        where prod_name regexp '[0-9][0-9][0-9][0-9]' 
        order by prod_name;
        方法3：select prod_name from products 
        where prod_name regexp '[[:digit:]]{4}' 
        order by prod_name;
        总结[[:digit:]]=[0-9]，也就是说[:digit:]表示范围0-9，[]表示任选一个
    6.聚集函数：运行在多行，返回一个结果的函数
    7.分组数据：
       7.1 group by语句所依赖的字段一定比查找的字段多（聚集函数的语句除外）
       7.2 经过group by之后，表中的字段仅剩group by所依赖的字段和count(*)信息，所以
           select 语句的范围只有group by所依赖的字段和count(*)信息
       7.3 select cust_name,
       (select count(*) from orders where orders.cust_id = customers.cust_id) as orders 
       from customers order by cust_name;
         先从外层查询得到cust_id,传入子查询得到结果，在将结果返回显示？
         将子查询理解为函数调用可能好一点
21.HBase
    1.启动HBase：在/usr/local/hbase/bin下sudo ./start-hbase.sh
    2.验证启动成功：在浏览器查看http://localhost:16010
### 相关博客
1.hiveUDF的相关博客
 - hiveUDF相关
   https://blog.csdn.net/sheep8521/article/details/81001893
# 金融大数据处理技术实验四——Spark编程

**211275032 汪科**

**使用IDEA IDE，Scala语言，Spark 3.5.0，SDK 2.12.18，在MacbookPro M2 Silicon上编程**

## Task1

### Task1.1

```scala
def task1_1(): Unit = {
  //任务1
  val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
  val sc= new SparkContext(conf)
  val fileRDD=sc.textFile("/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/application_data.csv")
  val header = fileRDD.first()
  val dataRDD = fileRDD.filter(row => row != header)

  val counts = dataRDD.map { (line: String) =>
    val parts = line.split(",")
    val value = parts(7).toDouble
    val bin = (value / 10000).toInt
    ((bin * 10000, (bin + 1) * 10000), 1)
  }.reduceByKey(_ + _)


  val sortedCounts = counts.sortByKey()
  val formattedCounts = sortedCounts.map {
    case ((lower, upper), count) => s"[$lower, $upper]: $count"
  }

  val outputPath = "/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/mac-spark/output/task1_1"
  formattedCounts.saveAsTextFile(outputPath)
}
```

* 前5行的功能主要是读取数据并储存为RDD格式，同时去掉第一行column_name。

* 通过map函数将每一行分割（因为是csv格式），将AMT_CREDIT（对应第7列）的区间，值为1，然后最后加总（桶分类，类似mapreduce）。
* 排序后整成字符串再输出

#### 运行截图

![image-20231226224656214](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226224656214.png)

#### WebUI运行结果

![image-20231226224624578](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226224624578.png)

#### 结果（部分）

![image-20231226224749116](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226224749116.png)

### Task1.2

```scala
def task1_2(): Unit = {
  val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
  val sc= new SparkContext(conf)
  val fileRDD=sc.textFile("/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/application_data.csv")
  val header = fileRDD.first()
  val dataRDD = fileRDD.filter(row => row != header)

  val diffRDD = dataRDD.map(row => {
    val cols = row.split(",")
    val diff = cols(7).toDouble - cols(6).toDouble
    (row, diff)，原始数据作为值创建一个新的键值对RDD
  val diffPairRDD = diffRDD.map(x => (x._2, x))

  // 对键值对RDD进行排序
  val sortedRDD = diffPairRDD.sortByKey(false)

  // 获取最高的10条记录和最低的10条记录
  val top10 = sortedRDD.take(10).map(_._2)
  val bottom10 = sortedRDD.sortByKey(true).take(10).map(_._2)

  // 保存到指定路径的文件中
  sc.parallelize(top10).saveAsTextFile("/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/mac-spark/output/task1_2/top10")
  sc.parallelize(bottom10).saveAsTextFile("/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/mac-spark/output/task1_2/bottom10")
  try Thread.sleep(100000)
  catch {
    case e: InterruptedException =>
    throw new RuntimeException(e)
  }
  sc.stop()
}
```

* 前5行的功能主要是读取数据并储存为RDD格式，同时去掉第一行column_name。

* 对指定列作差值，并作为新的一列添加到原来的RDD中
* 使用差值列作为键，记录作为值进行基于键的排序（2次），取高10位低10位

#### WebUI运行结果

![image-20231226225119135](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226225119135.png)

#### 结果

**高10位**

![image-20231226225226491](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226225226491.png)

**低10位**

![image-20231226225325725](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226225325725.png)

## Task2

### Task2.1

```Scala
def task2_1(): Unit = {
  val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
  val sc = new SparkContext(conf)
  val fileRDD = sc.textFile("/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/application_data.csv")
  val header = fileRDD.first()
  val dataRDD = fileRDD.filter(row => row != header)
  val maleData = dataRDD.filter(line => line.split(",")(2) == "M")
  val maleNum = maleData.count()
  val childrenCounts = maleData.map { (line: String) =>
    val parts = line.split(",")
    val childrenCnt = parts(5).toInt
    (childrenCnt, 1)
  }.reduceByKey(_ + _)
  val sortedCounts = childrenCounts.sortByKey()
  val res=sortedCounts.filter{case (k,v)=>k!=0}.map{case (k,v)=>(k,v.toDouble/maleNum)}
  val outputPath = "/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/mac-spark/output/task2_1"
  res.saveAsTextFile(outputPath)
  try Thread.sleep(100000)
  catch {
    case e: InterruptedException =>
    throw new RuntimeException(e)
  }
  sc.stop()
}
```

* 前5行的功能主要是读取数据并储存为RDD格式，同时去掉第一行column_name。
* 使用filter过滤器选取男性，并将条目数保存在一个变量maleNum中
* 将每一列转为一个键值对：（孩子数量，1），并通过reduceByKey进行聚合
* sort后再使用过滤器将数量变为占比

#### WebUI运行结果

![image-20231226225656062](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226225656062.png)

#### 结果

![image-20231226225717164](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226225717164.png)

![image-20231226225725975](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226225725975.png)

![image-20231226225732988](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226225732988.png)

### Task2.2

```Scala
def task2_2(): Unit = {
  val spark=SparkSession.builder().appName("Spark Pi").master("local").getOrCreate()
  val sc = spark.sparkContext
  val fileRDD = sc.textFile("/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/application_data.csv")
  val header = fileRDD.first()
  val dataRDD = fileRDD.filter(row => row != header)
  val avgIncomeRDD = dataRDD.map(row => {
    val cols = row.split(",")
    val diff = cols(6).toDouble / -cols(13).toDouble
    (cols(0), diff)
  })
  val filteredAvgIncomeRDD = avgIncomeRDD.filter {
    case (row, diff) => diff > 1
  }
  val sortedAvgIncomeRDD = filteredAvgIncomeRDD.sortBy(_._2, false)
  import spark.implicits._
  val df=sortedAvgIncomeRDD.toDF("SK_ID_CURR","avg_income")
  //保存为csv
  val outputPath = "/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/mac-spark/output/task2_2"
  df.write.option("header", "true").csv(outputPath)
  try Thread.sleep(100000)
  catch {
    case e: InterruptedException =>
    throw new RuntimeException(e)
  }
  sc.stop()
}
```

**`SparkSession`是Spark 2.0引入的新概念，它封装了`SparkContext`和`SQLContext`，并且提供了DataFrame和Dataset API。**

* 前5行的功能主要是读取数据并储存为RDD格式，同时去掉第一行column_name。

* 使用map获取一个新的Tuple RDD：（SK_ID_CURR, avg_income)

* 通过filter配合case选出avg_income>1的条目并排序

* 将RDD转为一个sql.Dataframe类型变量并赋予列名

* 保存

#### WebUI运行结果

![image-20231226230230210](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226230230210.png)

#### 结果（部分）

![image-20231226230305553](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226230305553.png)

## Task3

```Scala
def task3():Unit={
  import org.apache.spark.ml.classification.LogisticRegression
  import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
  import org.apache.spark.ml.feature.VectorAssembler
  import org.apache.spark.sql.SparkSession

  val spark=SparkSession.builder().appName("Spark Pi").master("local").getOrCreate()

  // 读取预处理过的数据
  val inputPath= "/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/application_data_preprocessed.csv"
  val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(inputPath)
  // 获取你的特征列名
  val featureColumns = data.columns.filter(_ != "TARGET")
  print(featureColumns.mkString("Array(", ", ", ")"))
  // 创建向量装配器
  val assembler = new VectorAssembler()
  .setInputCols(featureColumns)
  .setOutputCol("features")
  .setHandleInvalid("skip")
  // 将数据框转换为特征向量
  val dfWithFeatures = assembler.transform(data)
  // 将数据分为训练数据和测试数据
  val Array(trainingData, testData) = dfWithFeatures.randomSplit(Array(0.8, 0.2))
  //LR
  // 创建一个Logistic Regression模型实例
  val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol("TARGET")
  // 训练模型
  val model1 = lr.fit(trainingData)
  // 使用模型来预测测试数据
  val predictions = model1.transform(testData)
  // 创建一个评估器
  val evaluator = new MulticlassClassificationEvaluator().setLabelCol("TARGET").setPredictionCol("prediction").setMetricName("accuracy")
  // 计算测试数据的准确率
  val accuracy = evaluator.evaluate(predictions)
  println(s"Test Accuracy = $accuracy")
  //决策树
  import org.apache.spark.ml.classification.DecisionTreeClassifier
  // 创建一个Decision Tree模型实例
  val dt = new DecisionTreeClassifier().setLabelCol("TARGET").setFeaturesCol("features")
  // 训练模型
  val model2 = dt.fit(trainingData)
  // 使用模型来预测测试数据
  val predictions2 = model2.transform(testData)
  // 计算测试数据的准确率
  val accuracy2 = evaluator.evaluate(predictions2)
  println(s"Test Accuracy = $accuracy2")
  //随机森林
  import org.apache.spark.ml.classification.RandomForestClassifier
  // 创建一个Random Forest模型实例
  val rf = new RandomForestClassifier().setLabelCol("TARGET").setFeaturesCol("features")
  // 训练模型
  val model3 = rf.fit(trainingData)
  // 使用模型来预测测试数据
  val predictions3 = model3.transform(testData)
  // 计算测试数据的准确率
  val accuracy3 = evaluator.evaluate(predictions3)
  println(s"Test Accuracy = $accuracy3")
  val outputPath = "/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/mac-spark/output/task3"
  val res=spark.sparkContext.parallelize(Seq(("Logistic Regression",accuracy),("Decision Tree",accuracy2),("Random Forest",accuracy3)))
  res.saveAsTextFile(outputPath)
  try Thread.sleep(100000)
  catch {
    case e: InterruptedException =>
    throw new RuntimeException(e)
  }
  spark.stop()
}
```

这里使用了lab2中已经预处理好的数据。

* 创建Spark会话：使用`SparkSession.builder()`创建一个Spark会话
* 特征列提取：从数据中提取所有的特征列，即除"TARGET"列以外的所有列。
* 创建VectorAssembler：VectorAssembler是一个转换器，将给定的多列数据转换为单个向量列，用于机器学习算法的输入。这里设定所有特征列为输入列，输出列名为"features"，并设定处理无效数据的方式为跳过。
* 数据转换：使用VectorAssembler将数据转换为特征向量。
* 数据切分：将转换后的数据随机切分为80%的训练数据和20%的测试数据。

使用三种不同的机器学习模型进行训练和预测：

* 逻辑回归：创建逻辑回归模型，设置最大迭代次数为10，正则化参数为0.3，弹性网络参数为0.8，标签列为"TARGET"，然后使用训练数据训练模型，接着使用训练好的模型对测试数据进行预测。
* 决策树：创建决策树模型，设置标签列为"TARGET"，特征列为"features"，然后使用训练数据训练模型，接着使用训练好的模型对测试数据进行预测。
* 随机森林：创建随机森林模型，设置标签列为"TARGET"，特征列为"features"，然后使用训练数据训练模型，接着使用训练好的模型对测试数据进行预测。

对于每种模型，都使用MulticlassClassificationEvaluator计算预测的准确率，并打印出来。

### WebUI运行结果

![image-20231226231157655](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226231157655.png)

### 结果

![image-20231226231227323](https://repo-for-md.oss-cn-beijing.aliyuncs.com/uPic/image-20231226231227323.png)

## Obstacle&Solution

* Windows下该版本怎么都配不好（提示缺少某个包的定义等），同学用完全相同的包和操作步骤没有出现问题，气的我直接放弃window转战mac，一下就好了

* 在Task3的这段代码中：

  ```Scala
  val inputPath= "/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/application_data_preprocessed.csv"
  val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(inputPath)
  // 获取你的特征列名
  val featureColumns = data.columns.filter(_ != "TARGET")
  // 创建向量装配器
  val assembler = new VectorAssembler()
  .setInputCols(featureColumns)
  .setOutputCol("features")
  .setHandleInvalid("skip")
  // 将数据框转换为特征向量
  val dfWithFeatures = assembler.transform(data)
  // 将数据分为训练数据和测试数据
  val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))
  //LR
  // 创建一个Logistic Regression模型实例
  val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol("TARGET")
  // 训练模型
  val model1 = lr.fit(trainingData)
  ```

  执行到训练模型时总是报错。后来发现原因是：在训练模型时使用了原始数据`trainingData`，而不是经过`VectorAssembler`转换后的`dfWithFeatures`。

  将

  ```Scala
  // 将数据分为训练数据和测试数据
  val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))
  ```

  修改为

  ```Scala
  // 将数据分为训练数据和测试数据
  val Array(trainingData, testData) = dfWithFeatures.randomSplit(Array(0.8, 0.2))
  ```

  即可。
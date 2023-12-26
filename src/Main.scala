import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object Main {
  def main(args: Array[String]): Unit = {
    //task1_1()
    //task1_2()
    //task2_1()
    //task2_2()
    task3()
  }

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
    try Thread.sleep(100000)
    catch {
      case e: InterruptedException =>
        throw new RuntimeException(e)
    }
    sc.stop()
  }

  def task1_2(): Unit = {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc= new SparkContext(conf)
    val fileRDD=sc.textFile("/Users/dragonk/Library/CloudStorage/OneDrive-个人/NJU workspace/Grade3_1/FBDP/实验/lab4/application_data.csv")
    val header = fileRDD.first()
    val dataRDD = fileRDD.filter(row => row != header)

    val diffRDD = dataRDD.map(row => {
      val cols = row.split(",")
      val diff = cols(7).toDouble - cols(6).toDouble
      (row, diff)
    })
    // 使用差值作为键，原始数据作为值创建一个新的键值对RDD
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
}
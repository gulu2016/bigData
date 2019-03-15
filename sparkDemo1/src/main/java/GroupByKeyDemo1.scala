import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local[3]")

    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 20)
    val rdd2 = rdd1.map(e=>{
      val tname = Thread.currentThread().getName()
      println(tname)
    })

    rdd2.collect()
  }
}

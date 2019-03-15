package Api_Join_Demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: ApiDemo
  * @ClassName: joinDemo
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-1-4 下午3:48
  * @UpdateUser: 更新者
  * @UpdateDate: 19-1-4 下午3:48
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object joinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    //名单RDD
    val namesRDD1 = sc.textFile("/home/zhangjiaqian/IdeaProjects/scalaWordCount" +
      "/sparkDemo1/src/main/java/Api_Join_Demo/names")
    val namesRDD2 = namesRDD1.map(line => {
      var arr = line.split(" ")
      (arr(0).toInt,arr(1))
    })

    //总成绩
    val scoreRDD1 = sc.textFile("/home/zhangjiaqian/IdeaProjects/scalaWordCount" +
      "/sparkDemo1/src/main/java/Api_Join_Demo/scores")
    val scoreRDD2 = scoreRDD1.map(line => {
      var arr = line.split(" ")
      (arr(0).toInt,arr(1).toInt)
    })

    val rdd = namesRDD2.join(scoreRDD2)
    rdd.collect().foreach(t =>{
      println(t._1+" : "+t._2)
    })
    //将rdd的结果保存到文件
    rdd.saveAsTextFile("/home/zhangjiaqian/IdeaProjects" +
      "/scalaWordCount/sparkDemo1/src/main/java/Api_Join_Demo/out")
  }
}

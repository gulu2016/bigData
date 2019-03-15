package sanbuquFiles.unit12.Movie_Users_Analyzer_RDD_12_5

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12.Movie_Users_Analyzer_RDD_12_5
  * @ClassName: SecondarySortingKeyScalaTest
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-2-27 下午4:21
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-27 下午4:21
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object SecondarySortingKeyScalaTest {
  def main(args: Array[String]): Unit = {
    val dataPath = "/home/zhangjiaqian/sanbuqu/"
    //本地执行，并设置程序名字
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Movie_Users_Analyzer")

    //获取sparkcontext
    val sc = new SparkContext(conf)

    //设置日志运行级别
    sc.setLogLevel("ERROR")

    //把数据加载进来转换为RDD
    val usersRDD = sc.textFile(dataPath+"users.dat")
    val moviesRDD = sc.textFile(dataPath+"movies.dat")
    val ratingsRDD = sc.textFile(dataPath+"ratings.dat")

    println("对电影评分以TimeStamp和Rating两个维度进行二次降序排列：")
    val pairWithSortkey = ratingsRDD.map(line => {
      val splited = line.split("::")
      (new SecondarySortingKeyScala.SecondarySortKey(splited(3).toDouble
        ,splited(2).toDouble),line)
      })
    val sorted = pairWithSortkey.sortByKey(false);
    val sortedResult = sorted.map(sortedline => sortedline._2)
    sortedResult.take(10).foreach(println)
  }
}

package sanbuquFiles

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ProjectName: Default (Template) Project
  * @Package: sanbuquFiles
  * @ClassName: sanbuqu1
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-1-2 上午10:59
  * @UpdateUser: 更新者
  * @UpdateDate: 19-1-2 上午10:59
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object sanbuqu1 {
  class SecondarySortKey(val first:Double,val second:Double)
    extends Ordered[SecondarySortKey] with Serializable{
    override def compare(other: SecondarySortKey): Int = {
      if(this.first - other.first != 0){
        (this.first - other.first).toInt
      }
      else {
        if(this.second - other.second > 0){
          Math.ceil(this.second - other.second).toInt
        }else if(this.second - other.second < 0){
          Math.floor(this.second - other.second).toInt
        }else{
          (this.second - other.second).toInt
        }
      }
    }
  }
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

    //具体数据处理的业务逻辑
    println("对电影评分数据以Timestamp和Rating两个维度进行二次降序排列：")
    val pairWithSortKey = ratingsRDD.map(line => {
      val splited = line.split("::")
      (new SecondarySortKey(splited(3).toDouble,splited(2).toDouble),line)
    })

    //直接调用sortByKey,此时会按照之前实现的compart方法排序
    val sorted = pairWithSortKey.sortByKey(false)

    val sortedResult = sorted.map(sorted => sorted._2)
    sortedResult.take(10).foreach(println)
  }
}

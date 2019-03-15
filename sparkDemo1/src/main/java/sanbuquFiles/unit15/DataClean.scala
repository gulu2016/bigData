package sanbuquFiles.unit15

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.StatCounter

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit15
  * @ClassName: DataClean
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-11 上午9:36
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-11 上午9:36
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object DataClean {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val conf = new SparkConf().setMaster("local[*]").setAppName("NBAPlayer_Analyzer_DateSet")
//    val spark = SparkSession
//      .builder()
//      .config(conf)
//      .getOrCreate()
//    val sc = spark.sparkContext
//
//
//    val statsPerYear = sc.textFile("/home/zhangjiaqian/IdeaProjects/scalaWordCount/sparkDemo1/src/main/java/sanbuquFiles/unit15/NBA.csv")
//    statsPerYear.filter(_.contains(",")).map(line => (2016,line))
//      .saveAsTextFile(s"/home/zhangjiaqian/IdeaProjects/scalaWordCount/sparkDemo1/src/main/java/sanbuquFiles/unit15/dataCleaned/")
//    val NBAStats = sc.textFile("/home/zhangjiaqian/IdeaProjects/scalaWordCount/sparkDemo1/src/main/java/sanbuquFiles/unit15/dataCleaned/*")
//    //去除csv文件的第一行
//    val filteredData = NBAStats.filter(line => !line.contains("TS%"))
//      .filter(line => line.contains(","))
//      .map(line => line.replace(",,",",0,"))
//    filteredData.collect().take(10).foreach(println(_))
//    filteredData.persist(StorageLevel.MEMORY_AND_DISK)
//
//    class BballStatCounter extends Serializable{
//      val stats:StatCounter = new StatCounter()
//      var missing:Long = 0
//
//      def add(x:Double):BballStatCounter = {
//        if(x.isNaN)
//          missing += 1
//        else
//          stats.merge(x)
//        this
//      }
//
//      def merge(other:BballStatCounter):BballStatCounter = {
//        stats.merge(other.stats)
//        missing += other.missing
//        this
//      }
//
//      def printStats(delim:String):String = {
//        stats.count + delim + stats.mean+delim+stats.stdev+delim+
//        stats.max+delim+stats.min
//      }
//
//      override def toString: String = {
//        "stats: "+stats.toString()+" NaN "+missing
//      }
//    }
//
//    object BballStatCounter extends Serializable{
//      def apply(x:Double)= new BballStatCounter().add(x)
//    }
//
//    def processState(stats0:org.apache.spark.rdd.RDD[String],
//                     txtStat:Array[String],
//                     bStats:scala.collection.Map[String,Double]=Map.empty,
//                     zStats:scala.collection.Map[String,Double] = Map.empty):RDD[(String,Double)] = {
//      val stats1:RDD[BballData] = stats0.map(x => bbParse(x,bStats,zStats))
//      val stats2 = {
//        if(bStats.isEmpty){
//          stats1.keyBy(x => x.year).map(x => (x._1,x._2.stats)).groupByKey()
//        }else{
//          stats1.keyBy(x => x.year).map(x => (x._1,x._2.statsZ)).groupByKey()
//        }
//      }
//      val stats3 = stats2.map{
//        case (x,y) => (x,y.map(a => a.map(b => BballStatCounter(b))))
//      }
//      val stats4 = stats3.map{
//        case (x,y) => (x,y.reduce((a,b) => a.zip(b).map{
//          case(c,d) => c.merge(d)
//        }))
//      }
//      val stats5 = stats4.map{
//        case(x,y) => (x,txtStat.zip(y))}.map{
//        x => (x._2.map{
//          case(y,z) => (x._1,y,z)
//        })
//      }
//
//      val stats6 = stats5.flatMap(x => x.map(y => (y._1,y._2,y._3.printStats(","))))
//
//
//    }
//  }
}

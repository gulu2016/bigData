package sanbuquFiles.unit12.Movie_Users_Analyzer_RDD_12_5

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12.Movie_Users_Analyzer_RDD_12_5
  * @ClassName: SecondarySortingKeyScala
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-2-27 下午4:15
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-27 下午4:15
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object SecondarySortingKeyScala {
  class SecondarySortKey(val first:Double,val second:Double)
    extends Ordered [SecondarySortKey] with Serializable{
    override def compare(that: SecondarySortKey): Int = {
      if(this.first - that.first != 0)
        (this.first - that.first).toInt
      else {
        if(this.second - that.second > 0)
          Math.ceil(this.second - that.second).toInt
        else if (this.second - that.second < 0)
          Math.floor(this.second - that.second).toInt
        else
          (this.second - that.second).toInt
      }
    }
  }
}

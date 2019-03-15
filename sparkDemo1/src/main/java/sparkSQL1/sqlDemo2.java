package sparkSQL1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;
import java.util.function.Consumer;

/**
 * @ProjectName: scalaWordCount
 * @Package: sparkSQL1
 * @ClassName: sqlDemo1
 * @Description: sparkSQL使用JDBC操纵Mysql数据库
 * @Author: gulu
 * @CreateDate: 19-1-8 上午10:44
 * @UpdateUser: 更新者
 * @UpdateDate: 19-1-8 上午10:44
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class sqlDemo2 {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SQLJava");

        SparkSession session = SparkSession.builder()
                .appName("SQLJava")
                //设置master
                .config("spark.master","local")
                .getOrCreate();

        String url = "jdbc:mysql://localhost:3306/day33_user";
        String table = "users";
        //配置jdbc的连接信息
        Dataset<Row> jdbcDF = session.read()
                .format("jdbc")
                .option("url",url)
                .option("dbtable",table)
                .option("user","root")
                .option("password","erha")
                .option("driver","com.mysql.jdbc.Driver")
                .load();
        jdbcDF.show();

        Dataset<Row> jdbcDF2 = jdbcDF.where("id > 2");
        jdbcDF2.show();
    }
}

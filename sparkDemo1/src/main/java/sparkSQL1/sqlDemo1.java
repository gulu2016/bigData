package sparkSQL1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.function.Consumer;

/**
 * @ProjectName: scalaWordCount
 * @Package: sparkSQL1
 * @ClassName: sqlDemo1
 * @Description: 使用sparkSQL读取json文件，并打印数据框
 * @Author: gulu
 * @CreateDate: 19-1-8 上午10:44
 * @UpdateUser: 更新者
 * @UpdateDate: 19-1-8 上午10:44
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class sqlDemo1 {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SQLJava");

        SparkSession session = SparkSession.builder()
                .appName("SQLJava")
                //设置master
                .config("spark.master","local")
                .getOrCreate();

        Dataset<Row> df1 = session.read().json("file:///home/zhangjiaqian/wordcount" +
                "/json.dat");
        df1.show();
        //创建临时视图
        df1.createOrReplaceTempView("customer");

        //通过where条件查询
        Dataset<Row> df2 = df1.where("age > 23");
        df2.show();


        Dataset<Row> df3;
        //查询age大于23的结果集
        df3 = session.sql("select * from customer where age > 23");
        df3.show();

        //聚合查询
        Dataset<Row> dfCount = session.sql("select count(*) from customer");
        dfCount.show();

        //将结构化数据转化为rdd
        JavaRDD<Row> rdd = df1.toJavaRDD();
        rdd.collect().forEach(new Consumer<Row>() {
            //匿名内部类与接口回调
            public void accept(Row row) {
                String age = row.getString(0);
                String name = row.getString(1);
                String number = row.getString(2);
                System.out.println(age + ","+name+","+number);
            }
        });

        //数据保存,.mode方法可以设置保存模式
        df2.write().mode(SaveMode.Append).json("file:///home/zhangjiaqian/wordcount" +
                "/out");
    }
}

package sanbuquFiles.unit12.Movie_Users_Analyzer_RDD_12_5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @ProjectName: scalaWordCount
 * @Package: sanbuquFiles.unit12.Movie_Users_Analyzer_RDD_12_5
 * @ClassName: MovieUserAnalyzerTest
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-2-27 下午2:39
 * @UpdateUser: 更新者
 * @UpdateDate: 19-2-27 下午2:39
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 * 问题：mapToPair传入的参数是什么意思
 */
public class MovieUserAnalyzerTestRating {
    public static void main(String[] args){
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local[*]")
                        .setAppName("Movie_User_Analyzer")
        );
        JavaRDD<String> lines = sc.textFile("/home/zhangjiaqian/sanbuqu/ratings.dat");
        //先按时间戳排序，再按评分排序
        JavaPairRDD<SecondarySortingKey,String> keyvalues = lines.mapToPair(
                new PairFunction<String,SecondarySortingKey,String>(){
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<SecondarySortingKey, String> call(String line) throws Exception {
                        String[] splited = line.split("::");
                        SecondarySortingKey key = new SecondarySortingKey(
                                Integer.valueOf(splited[3]),
                                Integer.valueOf(splited[2]));
                        return new Tuple2<SecondarySortingKey,String>(key,line);
                    }
                }
        );
        //按key进行二次排序
        JavaPairRDD<SecondarySortingKey,String> sorted = keyvalues.sortByKey(false);
        //该函数的作用就是提取出排序结果的第二个字段，也就是JavaPairRDD<SecondarySortingKey,String>
        //中的String字段
        JavaRDD<String> result = sorted.map(
                new Function<Tuple2<SecondarySortingKey, String>, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(Tuple2<SecondarySortingKey, String> tuple) throws Exception {
                return tuple._2;
            }
        });
        List<String> collected = result.take(10);
        for(String item:collected)
            System.out.println(item);
    }
}

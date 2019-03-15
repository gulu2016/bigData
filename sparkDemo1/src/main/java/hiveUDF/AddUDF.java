package hiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ProjectName: scalaWordCount
 * @Package: hiveUDF
 * @ClassName: AddUDF
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-2-15 下午12:54
 * @UpdateUser: 更新者
 * @UpdateDate: 19-2-15 下午12:54
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class AddUDF extends UDF {
    public int evaluate(int a,int b){
        return a+b;
    }
    public int evaluate(int a,int b,int c){
        return a+b+c;
    }
}

package hiveUDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * @ProjectName: scalaWordCount
 * @Package: hiveUDF
 * @ClassName: UDFGender
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-2-20 下午9:10
 * @UpdateUser: 更新者
 * @UpdateDate: 19-2-20 下午9:10
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class UDFGender extends UDF{
    public String evaluate(String gender){
        if(gender.equals("M"))
            return "hey,boy";
        else
            return "hello,girl";
    }
}

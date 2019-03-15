package HadoopTextBook;

import org.apache.hadoop.conf.Configuration;

/**
 * @ProjectName: HadoopTextBook
 * @Package: PACKAGE_NAME
 * @ClassName: demo188
 * @Description: 读取xml文件中的配置信息,有问题，暂时结果是null，也就是读取失败
 * @Author: gulu
 * @CreateDate: 18-12-12 上午9:16
 * @UpdateUser: 更新者
 * @UpdateDate: 18-12-12 上午9:16
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class demo188 {
    public static void main(String[] args){
        Configuration conf = new Configuration();
        conf.addResource("HadoopTextBook/testXML.xml");
        conf.addResource("HadoopTextBook/testXML2.xml");

        System.out.println(conf.get("size"));
        System.out.println(conf.get("weight"));
    }
}

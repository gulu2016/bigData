package hiveDemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @ProjectName: scalaWordCount
 * @Package: hiveDemo
 * @ClassName: hiveDemo1
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-1-21 下午3:00
 * @UpdateUser: 更新者
 * @UpdateDate: 19-1-21 下午3:00
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class hiveDemo1 {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/mydb2");

    }
}

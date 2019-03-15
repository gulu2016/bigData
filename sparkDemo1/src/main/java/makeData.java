import java.util.Random;

/**
 * @ProjectName: scalaWordCount
 * @Package: PACKAGE_NAME
 * @ClassName: makeData
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-3-9 上午8:52
 * @UpdateUser: 更新者
 * @UpdateDate: 19-3-9 上午8:52
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class makeData {
    public static void main(String[] args){
        Random ra =new Random();
        int a = ra.nextInt(10)+1;
        int cnt = 0;
        for(int j = 0;j < 60;j++){
            int userLogTimes = ra.nextInt(9)+1;
            for(int i = 0;i < userLogTimes;i++){
                int month = ra.nextInt(9)+1;
                int day = ra.nextInt(19)+10;
                int hour = ra.nextInt(25)+1;
                int minute = ra.nextInt(59)+1;
                int sec = ra.nextInt(59)+1;
                int typeVal = (ra.nextInt(100)+1)%2;
                int consumedVal = 0;
                if(typeVal != 0)
                    consumedVal = ra.nextInt(300)+1;
                System.out.println("{\"logID\":"+cnt+",\"userID\":"+j+",\"time\":\"2016-"+month+"-"+day+" "+hour+":"+minute+":"+sec+"\","+"\"typed\":"+typeVal+","+"\"consumed\":"+consumedVal+"}");
                cnt++;
            }
        }

    }
}

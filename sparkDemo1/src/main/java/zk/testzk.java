package zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @ProjectName: scalaWordCount
 * @Package: zk
 * @ClassName: testzk
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-3-13 下午7:37
 * @UpdateUser: 更新者
 * @UpdateDate: 19-3-13 下午7:37
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class testzk {
    @Test
    public void ls() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("localhost:2181",5000,null);
        List<String> list = zk.getChildren("/",null);
        for(String s:list)
            System.out.println(s);
    }

    //设置节点数据
    @Test
    public void setData() throws Exception{
        ZooKeeper zk = new ZooKeeper("localhost:2181",5000,null);
        zk.setData("/a","tom".getBytes(),1);
    }

    //创建临时节点，只要连接断开，节点就会被删除
    @Test
    public void createTempNode() throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost:2181",5000,null);
        zk.create("/c","zhang".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    //创建临时节点，只要连接断开，节点就会被删除
    @Test
    public void listen() throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost:2181",5000,null);

        Stat st = new Stat();
        Watcher w = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("data changed!");
                try {
                    zk.getData("/a", this,null);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        byte[] data = zk.getData("/a", w,st);
        System.out.println(new String(data));
        while (true){
            Thread.sleep(1000);
        }
    }
}

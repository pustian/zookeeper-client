package tian.pusen.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2019/1/17.
 */
public class ZookeeperService {
    // 会话超时时间，设置为与系统默认时间一致ms
    private static final int SESSION_TIMEOUT = 30 * 1000;

    private static final String ZOOKEEPER_SERVER_1 = "192.168.1.97:2181";
    private static final String ZOOKEEPER_SERVER_2 = "192.168.1.98:2181";
    private static final String ZOOKEEPER_SERVER_3 = "192.168.1.99:2181";

    // 创建 ZooKeeper 实例
    private ZooKeeper zooKeeper;

    // 创建 Watcher 实例
    private Watcher watcher = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            // Watched事件
            System.out.println("WatchedEvent >>> " + watchedEvent.toString());
        }
    };

    // 初始化 ZooKeeper 实例
    public void instZookeeper() throws IOException {
        // 连接到ZK服务，多个可以用逗号分割写
        String connectString = ZOOKEEPER_SERVER_1 + "," + ZOOKEEPER_SERVER_2 + "," + ZOOKEEPER_SERVER_3;
        zooKeeper = new ZooKeeper(connectString, ZookeeperService.SESSION_TIMEOUT, this.watcher);
        // 连接的主机马上down掉的机器
        if (!zooKeeper.getState().equals(ZooKeeper.States.CONNECTED)) {
            while (true) {
                if (zooKeeper.getState().equals(ZooKeeper.States.CONNECTED)) {
                    break;
                }
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void operateZookeeper() throws IOException, KeeperException, InterruptedException {
        System.out.println("1. 创建 ZooKeeper 节点 ");
        System.out.println("    znode ： zoo_test1, 数据： my_data1，权限： OPEN_ACL_UNSAFE ，节点类型： Persistent");
        zooKeeper.create("/zoo_test1", "my_data1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println("2. 查看是否创建成功： ");
        Stat stat = new Stat();
        byte[] getDataBytes = zooKeeper.getData("/zoo_test1", watcher, stat);
        String getDataString = new String(getDataBytes);
        System.out.println(getDataString);

        // 前面一行我们添加了对/zoo2节点的监视，所以这里对/zoo2进行修改的时候，会触发Watch事件。
        System.out.println("3. 修改节点数据 ");
        zooKeeper.setData("/zoo_test1", "tianpusen".getBytes(), -1);
        getDataBytes = zooKeeper.getData("/zoo_test1", watcher, stat);
        getDataString = new String(getDataBytes);
        System.out.println(getDataString);

        // 这里再次进行修改，则不会触发Watch事件，这就是我们验证ZK的一个特性“一次性触发”，也就是说设置一次监视，只会对下次操作起一次作用。
        System.out.println("4.1. 再次修改节点数据 ");
        zooKeeper.setData("/zoo_test1", "shanhy20160310-ABCD".getBytes(), -1);
        System.out.println("4.2. 查看是否修改成功： ");
        getDataBytes = zooKeeper.getData("/zoo_test1", false, null);
        getDataString = new String(getDataBytes);
        System.out.println(getDataString);

        System.out.println("5. 删除节点 ");
        zooKeeper.delete("/zoo_test1", -1);

        System.out.println("6. 查看节点是否被删除： ");
        System.out.println(" 节点状态： [" + zooKeeper.exists("/zoo_test1", false) + "]");
    }

    public void closeZookeeper() throws InterruptedException {
        zooKeeper.close();
    }

}

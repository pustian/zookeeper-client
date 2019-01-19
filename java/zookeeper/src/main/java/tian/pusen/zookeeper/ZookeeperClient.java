package tian.pusen.zookeeper;

import org.I0Itec.zkclient.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2019/1/17.
 */
public class ZookeeperClient {

    private static final String ZOOKEEPER_SERVER_1 = "192.168.1.97:2181";
    private static final String ZOOKEEPER_SERVER_2 = "192.168.1.98:2181";
    private static final String ZOOKEEPER_SERVER_3 = "192.168.1.99:2181";


    /**
     * 订阅children变化
     */
    public static void childChangesListener(ZkClient zkClient, final String path) {
        zkClient.subscribeChildChanges(path, new IZkChildListener() {
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println("clildren of path " + parentPath + ":" + currentChilds);
            }
        });
    }

    /**
     * 订阅节点数据变化
     */
    public static void dataChangesListener(ZkClient zkClient, final String path) {
        zkClient.subscribeDataChanges(path, new IZkDataListener() {
            public void handleDataChange(String dataPath, Object data) throws Exception {
                System.out.println("Data of " + dataPath + " has changed.");
            }

            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println("Data of " + dataPath + " has changed.");
            }
        });
    }

    /**
     * 订阅状态变化
     */
    public static void stateChangesListener(ZkClient zkClient) {
        zkClient.subscribeStateChanges(new IZkStateListener() {
            public void handleStateChanged(KeeperState state) throws Exception {
                System.out.println("handleStateChanged");
            }

            public void handleSessionEstablishmentError(Throwable error) throws Exception {
                System.out.println("handleSessionEstablishmentError");
            }

            public void handleNewSession() throws Exception {
                System.out.println("handleNewSession");
            }
        });
    }

    public static void main(String[] args) {
        String connectString = ZOOKEEPER_SERVER_1 + "," + ZOOKEEPER_SERVER_2 + "," + ZOOKEEPER_SERVER_3;
        ZkClient zkClient = new ZkClient(connectString);
        String node = "/myapp";

        // 订阅监听事件
        childChangesListener(zkClient, node);
        dataChangesListener(zkClient, node);
        stateChangesListener(zkClient);

        System.out.println("Begin======");

        if (!zkClient.exists(node)) {
            zkClient.createPersistent(node, "hello zookeeper");
        }
        System.out.println(zkClient.readData(node));

        zkClient.updateDataSerialized(node, new DataUpdater<String>() {
            public String update(String currentData) {
                return currentData + "-123";
            }
        });
        System.out.println(zkClient.readData(node));

        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("======END");
    }


}

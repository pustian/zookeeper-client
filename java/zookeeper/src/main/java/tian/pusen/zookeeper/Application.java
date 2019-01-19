package tian.pusen.zookeeper;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Created by Administrator on 2019/1/17.
 */
public class Application {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        System.out.println("BEGIN ++++++");
        ZookeeperService zookeeperService = new ZookeeperService();
        zookeeperService.instZookeeper();
        zookeeperService.operateZookeeper();
        zookeeperService.closeZookeeper();
        System.out.println("++++++ END");
    }
}

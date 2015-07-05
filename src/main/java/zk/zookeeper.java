package zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

public class zookeeper {

	private int SESSION_TIMEOUT = 30000;

	private String HOST = "slave2:2181";

	private ZooKeeper zk;

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		zookeeper keeper = new zookeeper();
		keeper.getData();
	}

	public void zkApi() throws Exception {
		// 创建一个与服务器的连接
		zk = new ZooKeeper(HOST, SESSION_TIMEOUT, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});
		// 创建一个目录节点
		zk.create("/testRootPath", "testRootData".getBytes(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		// 创建一个子目录节点
		zk.create("/testRootPath/testChildPathOne",
				"testChildDataOne".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out
				.println(new String(zk.getData("/testRootPath", false, null)));
		// 取出子目录节点列表
		System.out.println(zk.getChildren("/testRootPath", true));
		// 修改子目录节点数据
		zk.setData("/testRootPath/testChildPathOne",
				"modifyChildDataOne".getBytes(), -1);
		System.out.println("目录节点状态：[" + zk.exists("/testRootPath", true) + "]");
		// 创建另外一个子目录节点
		zk.create("/testRootPath/testChildPathTwo",
				"testChildDataTwo".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println(new String(zk.getData(
				"/testRootPath/testChildPathTwo", true, null)));
		// 关闭连接
		zk.close();
	}

	public void zkDelete() throws Exception {
		// 创建一个与服务器的连接
		zk = new ZooKeeper(HOST, SESSION_TIMEOUT, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});
		// 删除子目录节点
		zk.delete("/testRootPath/testChildPathTwo", -1);
		zk.delete("/testRootPath/testChildPathOne", -1);
		// 删除父目录节点
		zk.delete("/testRootPath", -1);
		// 关闭连接
		zk.close();
	}

	/**
	 * 创建一个znode CreateMode 取值 PERSISTENT：持久化，这个目录节点存储的数据不会丢失
	 * PERSISTENT_SEQUENTIAL
	 * ：顺序自动编号的目录节点，这种目录节点会根据当前已近存在的节点数自动加1，然后返回给客户端已经成功创建的目录节点名；
	 * EPHEMERAL：临时目录节点，一旦创建这个节点的客户端与服务器端口也就是 session过期超时，这种节点会被自动删除
	 * EPHEMERAL_SEQUENTIAL：临时自动编号节点
	 * 
	 * @throws Exception
	 */
	public void testCreate1() throws Exception {
		zk = new ZooKeeper(HOST, SESSION_TIMEOUT, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});

		zk.addAuthInfo("digest", "guest:guest123".getBytes());

		zk.create("/zk001", "zk001data".getBytes(), Ids.CREATOR_ALL_ACL,
				CreateMode.PERSISTENT);

	}

	public void testCreate2() throws Exception {
		zk = new ZooKeeper(HOST, SESSION_TIMEOUT, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});

		List<ACL> acls = new ArrayList<ACL>(2);

		Id id1 = new Id("digest",
				DigestAuthenticationProvider.generateDigest("admin:admin123"));
		ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);

		Id id2 = new Id("digest",
				DigestAuthenticationProvider.generateDigest("guest:guest123"));
		ACL acl2 = new ACL(ZooDefs.Perms.READ, id2);

		acls.add(acl1);
		acls.add(acl2);

		zk.create("/zk002", "zk002data".getBytes(), acls, CreateMode.PERSISTENT);

	}

	public void getData() throws Exception {
		zk = new ZooKeeper(HOST, SESSION_TIMEOUT, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});
		zk.addAuthInfo("digest", "admin:admin123".getBytes());
		System.out.println(new String(zk.getData("/zk002", false, null)));
	}
}

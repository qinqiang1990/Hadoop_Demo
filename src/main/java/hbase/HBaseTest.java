package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.conf.*;

public class HBaseTest {

	private static Configuration conf = null;

	// private static Configuration cnf = new Configuration();

	/**
	 * 初始化配置
	 */
	static {
		/*
		 * cnf.set("hbase.zookeeper.quorum", "your_master");
		 * cnf.set("hbase.zookeeper.property.clientPort", "2181");
		 * 
		 * cconfiguration.set("hbase.zookeeper.property.clientPort", "2181");
		 * configuration.set("hbase.zookeeper.quorum", "xxx.xxx.xxx.xxx");
		 * configuration.set("hbase.master", "xxx.xxx.xxx.xxx:60000");
		 */
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.126.128");

	}

	/**
	 * 创建一张表
	 */
	public static void creatTable(String tableName, String[] familys)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}
	}

	/**
	 * 删除表
	 */
	public static void deleteTable(String tableName) throws Exception {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table " + tableName + " ok.");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 插入一行记录
	 */
	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) throws Exception {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除一行记录
	 */
	public static void delRecord(String tableName, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		List list = new ArrayList();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("del recored " + rowKey + " ok.");
	}

	/**
	 * 查找一行记录
	 */
	public static void getOneRecord(String tableName, String rowKey)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
	}

	/**
	 * 显示所有数据
	 */
	public static void getAllRecord(String tableName) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 显示所有数据
	 */
	public static void getScanRecord(String tableName,String startRow,String stopRow) {
		try {
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			s.setStartRow(startRow.getBytes());  
			s.setStopRow(stopRow.getBytes()); 
			s.setCaching(50);//1000  
			s.setCacheBlocks(false);  
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 高窄表
	 */
	public static void getHgih(String tableName) {
		try { 
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				System.out.println("table already exists!");
			}
			else {
				
				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				tableDesc.addFamily(new HColumnDescriptor("coloumn"));
				admin.createTable(tableDesc);

				HTable table = new HTable(conf, tableName);
				for (int i = 0; i < 10; i++) {
					Put put = new Put(Bytes.toBytes("rowKey"));
					put.add(Bytes.toBytes("coloumn"), Bytes.toBytes(String.valueOf(i)),
							Bytes.toBytes(String.valueOf(i)));
					table.put(put);
				}
			}
			HBaseTest.getAllRecord( tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			String tablename = "high";
			//HBaseTest.getHgih(tablename);
			//HBaseTest.getScanRecord(tablename,"zkb","zkc");
			
			/*
			String[] familys = { "grade", "course" };
			HBaseTest.creatTable(tablename, familys);
			*/
			// add record zkb
			/*
			HBaseTest.addRecord(tablename, "zkb2", "grade", "", "5");
			HBaseTest.addRecord(tablename, "zkb2", "course", "", "90");
			HBaseTest.addRecord(tablename, "zkb2", "course", "math", "97");
			HBaseTest.addRecord(tablename, "zkb2", "course", "art", "87");
			// add record baoniu
			HBaseTest.addRecord(tablename, "baoniu2", "grade", "", "4");
			HBaseTest.addRecord(tablename, "baoniu2", "course", "math", "89");*/

			/*
			 * System.out.println("===========get one record========");
			 * HBaseTest.getOneRecord(tablename, "zkb");
			 * 
			 * System.out.println("===========show all record========");
			 * HBaseTest.getAllRecord(tablename);
			 * 
			 * System.out.println("===========del one record========");
			 * HBaseTest.delRecord(tablename, "baoniu");
			 * HBaseTest.getAllRecord(tablename);
			 */
			/*System.out.println("===========show all record========");*/
			HBaseTest.getAllRecord(tablename);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	

}

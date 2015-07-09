package coprocessor.Endpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

//http://ju.outofmemory.cn/entry/17708
public class MyAggregationClient {

//	private static final byte[] TABLE_NAME = Bytes.toBytes("persion");
	private static final TableName TABLE_NAME = TableName.valueOf("persion");
			
	private static final byte[] CF = Bytes.toBytes("id");

	public static void main(String[] args) throws Throwable {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.126.128");
		// 提高RPC通信时长
		// configuration.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		// configuration.setLong("hbase.client.scanner.caching", 1000);

		AggregationClient aggregationClient = new AggregationClient(
				configuration);
		Scan scan = new Scan();
		// 指定扫描列族，唯一值
		scan.addFamily(CF);

		LongColumnInterpreter ci = new LongColumnInterpreter();

		long rowCount = aggregationClient.rowCount(TABLE_NAME, ci, scan);
		System.out.println("row count is " + rowCount);
		// aggregationClient.avg(TABLE_NAME, ci, scan);

	}
}
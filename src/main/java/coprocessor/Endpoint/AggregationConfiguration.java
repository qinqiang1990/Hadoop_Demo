package coprocessor.Endpoint;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
import org.apache.hadoop.hbase.util.Bytes;
//http://blog.csdn.net/xiao_jun_0820/article/details/25413919
//http://ju.outofmemory.cn/entry/17708
//http://blog.csdn.net/flyingpig4/article/details/7968217

public class AggregationConfiguration {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		byte[] tableName = Bytes.toBytes("persion");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.126.128");
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(tableName);

		HTableDescriptor htd = admin.getTableDescriptor(tableName);
		htd.addCoprocessor(AggregateImplementation.class.getName());
		// htd.addCoprocessor(AggregateImplementation.class.getName(), new
		// Path("hdfs://master68:8020/sharelib/aggregate.jar"), 1001, null);
		// htd.removeCoprocessor(RowCountEndpoint.class.getName());
		admin.modifyTable(tableName, htd);
		admin.enableTable(tableName);
		admin.close();
	}
}

/*<property>
<name >hbase.coprocessor.region.classes </name >
<value >org.apache.hadoop.hbase.coprocessor.AggregateImplementation </value >
</property >

(1)disable指定表。hbase> disable 'mytable'

(2)添加aggregation hbase> alter 'mytable', METHOD => 'table_att','coprocessor'=>'|org.apache.hadoop.hbase.coprocessor.AggregateImplementation||'

(3)重启指定表 hbase> enable 'mytable'

hbase shell添加coprocessor:

disable 'member'
alter 'member',METHOD => 'table_att','coprocessor' => 'hdfs://master24:9000/user/hadoop/jars/test.jar|mycoprocessor.SampleCoprocessor|1001|'
enable 'member'


hbase shell 删除coprocessor:

disable 'member'
alter 'member',METHOD => 'table_att_unset',NAME =>'coprocessor$1'
enable 'member'*/
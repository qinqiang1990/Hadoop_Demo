//http://blog.csdn.net/lifuxiangcaohui/article/details/39991183

//http://www.dengchuanhua.com/149.html

package coprocessor.Coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;

public class TestCoprocessor extends BaseRegionObserver {
	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final boolean writeToWAL)
			throws IOException {
		Configuration conf = new Configuration();
		HTable table = new HTable(conf, "index_table");
		List<KeyValue> kv = put.get("data".getBytes(), "name".getBytes());
		Iterator<KeyValue> kvItor = kv.iterator();
		while (kvItor.hasNext()) {
			KeyValue tmp = kvItor.next();
			Put indexPut = new Put(tmp.getValue());
			indexPut.add("index".getBytes(), tmp.getRow(),
					Bytes.toBytes(System.currentTimeMillis()));
			table.put(indexPut);
		}
		table.close();
	}
}
/*
 * 写完后要加载到table里面去，先把该文件打包成test.jar并上传到hdfs的/demo路径下，然后操作如下：
1. disable ‘testTable’

2. alter ‘testTable’, METHOD=>’table_att’,’coprocessor’=>’hdfs:///demo/test.jar|com.dengchuanhua.testhbase.TestCoprocessor|1001′

3. enable ‘testTable’

然后往testTable里面插数据就会自动往indexTableName写数据了。*/

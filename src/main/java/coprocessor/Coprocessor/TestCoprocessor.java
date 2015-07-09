package coprocessor.Coprocessor;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

//http://ju.outofmemory.cn/entry/17708
public class TestCoprocessor extends BaseRegionObserver {

	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, boolean writeToWAL) throws IOException {

		Configuration conf = new Configuration();
		HTable table = new HTable(conf, "index_table");
		List<Cell> kv = put.get("data".getBytes(), "name".getBytes());
		Iterator<Cell> kvItor = kv.iterator();
		while (kvItor.hasNext()) {
			Cell tmp = kvItor.next();
			Put indexPut = new Put(tmp.getValue());
			indexPut.add("index".getBytes(), tmp.getRow(),
					Bytes.toBytes(System.currentTimeMillis()));
			table.put(indexPut);
		}
		table.close();

	}
}
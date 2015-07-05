package hbase.key;

import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

public class Design {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long userid = 100L;

		byte[] bytes = Bytes.toBytes(userid);
		String hashPrefix = MD5Hash.getMD5AsHex(bytes).substring(0, 4);
		System.out.println(hashPrefix);

		byte[] bytes2 = Bytes.toBytes(hashPrefix);

		// rowkey取md5(userid)的前四位+userid.前四位用来散列userid,避免写入热点。缺点，不支持顺序scan
		// userId.
		byte[] rowkey = Bytes.add(bytes2, bytes);

		System.out.println(rowkey);

		// 可通过rowkey逆推得到 userid
		System.out.println(Bytes.toLong(rowkey, 4, rowkey.length - 4));

	}

	// 生成RowKey
	private String buildPutRowKey(int userId, Date addTime) {
		String key = userId + "_" + getRowKeyTimestamp(addTime) + "_"
				+ System.nanoTime();
		return key;
	}

	// 构建DataTime
	private long getRowKeyTimestamp(Date addTime) {
		return Long.MAX_VALUE - addTime.getTime();
	}

}

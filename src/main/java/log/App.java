package log;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		String uri = "hdfs://192.168.126.128:9000/";
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), config);

		// 列出hdfs上/logs/input目录下的所有文件和目录
		FileStatus[] statuses = fs.listStatus(new Path("/qq/input/"));
		for (FileStatus status : statuses) {
			System.out.println(status.getPath());
		}

		// 在hdfs的/logs/input录下创建一个文件，并写入一行文本
		/*FSDataOutputStream os = fs.create(new Path("/qq/input/test.txt"));
		os.write("Hello World!".getBytes());
		os.flush();
		os.close();*/

		// 显示在hdfs的/logs/input下指定文件的内容
		FSDataInputStream is = fs.open(new Path("/qq/input/test.txt"));
		IOUtils.copyBytes(is, System.out, 1024, true);
	}
}

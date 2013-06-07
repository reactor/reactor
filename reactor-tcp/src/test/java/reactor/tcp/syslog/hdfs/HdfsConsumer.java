package reactor.tcp.syslog.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import reactor.fn.Consumer;
import reactor.tcp.encoding.syslog.SyslogMessage;

import java.io.IOException;

/**
 * @author Jon Brisbin
 */
public class HdfsConsumer implements Consumer<SyslogMessage> {

	private final FSDataOutputStream out;

	public HdfsConsumer(Configuration conf, String dir, String name) throws IOException {
		Path path = new Path(dir, name);
		FileSystem fs = path.getFileSystem(conf);
		out = fs.create(path, true);
	}

	@Override
	public void accept(SyslogMessage msg) {
		try {
			out.writeUTF(msg.toString());
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

}

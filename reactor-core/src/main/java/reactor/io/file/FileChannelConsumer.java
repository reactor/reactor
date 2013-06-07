package reactor.io.file;

import reactor.fn.Consumer;
import reactor.io.Buffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
public class FileChannelConsumer<T> implements Consumer<T> {

	private final FileOutputStream out;
	private final FileChannel      channel;
	private final AtomicLong position = new AtomicLong(0);

	public FileChannelConsumer(String dir, String basename) throws FileNotFoundException {
		File d = new File(dir);
		if (!d.exists()) {
			d.mkdirs();
		}

		String filename = basename + "-" + System.currentTimeMillis();
		out = new FileOutputStream(new File(d, filename));
		channel = out.getChannel();
	}

	@Override
	public void accept(T t) {
		try {
			Buffer b = Buffer.wrap(t.toString());
			channel.write(b.byteBuffer(), position.getAndAdd(b.remaining()));
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

}

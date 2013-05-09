package reactor.support;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Stephane Maldini (smaldini)
 * @date: 4/2/13
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class QueueFactory {

	static {
		try {
			QUEUE_TYPE = (Class<? extends BlockingQueue>) Class.forName("java.util.concurrent.LinkedTransferQueue");
		} catch (ClassNotFoundException e) {
			QUEUE_TYPE = LinkedBlockingQueue.class;
		}
	}

	private static Class<? extends BlockingQueue> QUEUE_TYPE;

	public static <D> BlockingQueue<D> createQueue() {
		try {
			return (BlockingQueue<D>) QUEUE_TYPE.newInstance();
		} catch (Throwable t) {
			return new LinkedBlockingQueue<D>();
		}
	}
}

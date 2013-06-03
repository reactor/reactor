package reactor.tcp.netty;

import io.netty.channel.EventLoop;
import reactor.fn.Supplier;
import reactor.fn.cache.Cache;
import reactor.fn.cache.LoadingCache;
import reactor.fn.dispatch.AbstractDispatcher;
import reactor.fn.dispatch.Task;

/**
 * @author Jon Brisbin
 */
@SuppressWarnings({"rawtypes"})
public class NettyEventLoopDispatcher extends AbstractDispatcher {

	private final EventLoop   eventLoop;
	private final Cache<Task> readyTasks;

	public NettyEventLoopDispatcher(EventLoop eventLoop, int backlog) {
		this.eventLoop = eventLoop;
		this.readyTasks = new LoadingCache<Task>(
				new Supplier<Task>() {
					@Override
					public Task get() {
						return new NettyEventLoopTask();
					}
				},
				backlog,
				200
		);
	}

	@Override
	public void shutdown() {
		eventLoop.shutdown();
		super.shutdown();
	}

	@Override
	public void halt() {
		eventLoop.shutdownNow();
		super.halt();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <T> Task<T> createTask() {
		Task t = readyTasks.allocate();
		return (null != t ? t : new NettyEventLoopTask());
	}

	private final class NettyEventLoopTask extends Task<Object> implements Runnable {
		@Override
		public void submit() {
			eventLoop.execute(this);
		}

		@Override
		public void run() {
			try {
				execute();
			} finally {
				readyTasks.deallocate(this);
			}
		}
	}

}

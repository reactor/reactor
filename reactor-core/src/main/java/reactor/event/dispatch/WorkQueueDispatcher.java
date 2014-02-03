package reactor.event.dispatch;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.function.Consumer;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of a {@link Dispatcher} that uses a multi-threaded, multi-producer {@link RingBuffer} to queue tasks
 * to execute.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class WorkQueueDispatcher extends AbstractMultiThreadDispatcher {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ExecutorService           executor;
	private final Disruptor<WorkQueueTask>  disruptor;
	private final RingBuffer<WorkQueueTask> ringBuffer;

	@SuppressWarnings("unchecked")
	public WorkQueueDispatcher(String name,
	                           int poolSize,
	                           int backlog,
	                           final Consumer<Throwable> uncaughtExceptionHandler) {
		super(poolSize, backlog);
		this.executor = Executors.newFixedThreadPool(
				poolSize,
				new NamedDaemonThreadFactory(name, getContext())
		);
		this.disruptor = new Disruptor<WorkQueueTask>(
				new EventFactory<WorkQueueTask>() {
					@Override
					public WorkQueueTask newInstance() {
						return new WorkQueueTask();
					}
				},
				backlog,
				executor,
				ProducerType.MULTI,
				new BlockingWaitStrategy()
		);

		this.disruptor.handleExceptionsWith(new ExceptionHandler() {
			@Override
			public void handleEventException(Throwable ex, long sequence, Object event) {
				handleOnStartException(ex);
			}

			@Override
			public void handleOnStartException(Throwable ex) {
				if(null != uncaughtExceptionHandler) {
					uncaughtExceptionHandler.accept(ex);
				} else {
					log.error(ex.getMessage(), ex);
				}
			}

			@Override
			public void handleOnShutdownException(Throwable ex) {
				handleOnStartException(ex);
			}
		});

		WorkHandler<WorkQueueTask>[] workHandlers = new WorkHandler[poolSize];
		for(int i = 0; i < poolSize; i++) {
			workHandlers[i] = new WorkHandler<WorkQueueTask>() {
				@Override
				public void onEvent(WorkQueueTask task) throws Exception {
					task.run();
				}
			};
		}
		this.disruptor.handleEventsWithWorkerPool(workHandlers);

		this.ringBuffer = disruptor.start();
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		shutdown();
		try {
			executor.awaitTermination(timeout, timeUnit);
			disruptor.shutdown();
		} catch(InterruptedException e) {
			return false;
		}
		return true;
	}

	@Override
	public void shutdown() {
		executor.shutdown();
		disruptor.shutdown();
		super.shutdown();
	}

	@Override
	public void halt() {
		executor.shutdownNow();
		disruptor.halt();
		super.halt();
	}

	@Override
	protected Task allocateTask() {
		long seqId = ringBuffer.next();
		return ringBuffer.get(seqId).setSequenceId(seqId);
	}

	protected void execute(Task task) {
		ringBuffer.publish(((WorkQueueTask)task).getSequenceId());
	}

	@Override
	public void execute(final Runnable command) {
		ringBuffer.publishEvent(new EventTranslator<WorkQueueTask>() {
			@Override
			public void translateTo(WorkQueueTask event, long sequence) {
				command.run();
			}
		});
	}

	private class WorkQueueTask extends MultiThreadTask {
		private long sequenceId;

		public long getSequenceId() {
			return sequenceId;
		}

		public WorkQueueTask setSequenceId(long sequenceId) {
			this.sequenceId = sequenceId;
			return this;
		}
	}

}

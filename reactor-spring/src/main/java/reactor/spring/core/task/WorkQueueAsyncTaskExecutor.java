package reactor.spring.core.task;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import reactor.core.Environment;
import reactor.event.dispatch.AbstractLifecycleDispatcher;
import reactor.event.dispatch.WorkQueueDispatcher;
import reactor.function.Consumer;
import reactor.timer.Timer;

/**
 * Implementation of an {@link org.springframework.core.task.AsyncTaskExecutor} that is backed by a Reactor {@link
 * reactor.event.dispatch.WorkQueueDispatcher}.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class WorkQueueAsyncTaskExecutor extends AbstractAsyncTaskExecutor implements ApplicationEventPublisherAware {

	private final Logger log = LoggerFactory.getLogger(WorkQueueAsyncTaskExecutor.class);

	private ProducerType              producerType;
	private WaitStrategy              waitStrategy;
	private ApplicationEventPublisher eventPublisher;
	private WorkQueueDispatcher       workQueue;

	public WorkQueueAsyncTaskExecutor(Environment env) {
		this(env.getRootTimer());
	}

	public WorkQueueAsyncTaskExecutor(Timer timer) {
		super(timer);
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher eventPublisher) {
		this.eventPublisher = eventPublisher;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.workQueue = new WorkQueueDispatcher(
				getName(),
				getThreads(),
				getBacklog(),
				new Consumer<Throwable>() {
					@Override
					public void accept(Throwable throwable) {
						if(null != eventPublisher) {
							eventPublisher.publishEvent(new AsyncTaskExceptionEvent(throwable));
						} else {
							log.error(throwable.getMessage(), throwable);
						}
					}
				},
				(null != producerType ? producerType : ProducerType.MULTI),
				(null != waitStrategy ? waitStrategy : new BlockingWaitStrategy())
		);
	}

	@Override
	public WorkQueueAsyncTaskExecutor setName(String name) {
		return (WorkQueueAsyncTaskExecutor)super.setName(name);
	}

	@Override
	public WorkQueueAsyncTaskExecutor setThreads(int threads) {
		return (WorkQueueAsyncTaskExecutor)super.setThreads(threads);
	}

	@Override
	public WorkQueueAsyncTaskExecutor setBacklog(int backlog) {
		return (WorkQueueAsyncTaskExecutor)super.setBacklog(backlog);
	}


	/**
	 * Get the {@link com.lmax.disruptor.dsl.ProducerType} this {@link com.lmax.disruptor.RingBuffer} is using.
	 *
	 * @return the {@link com.lmax.disruptor.dsl.ProducerType}
	 */
	public ProducerType getProducerType() {
		return producerType;
	}

	/**
	 * Set the {@link com.lmax.disruptor.dsl.ProducerType} to use when creating the internal {@link
	 * com.lmax.disruptor.RingBuffer}.
	 *
	 * @param producerType
	 * 		the {@link com.lmax.disruptor.dsl.ProducerType}
	 *
	 * @return {@literal this}
	 */
	public WorkQueueAsyncTaskExecutor setProducerType(ProducerType producerType) {
		this.producerType = producerType;
		return this;
	}

	/**
	 * Get the {@link com.lmax.disruptor.WaitStrategy} this {@link com.lmax.disruptor.RingBuffer} is using.
	 *
	 * @return the {@link com.lmax.disruptor.WaitStrategy}
	 */
	public WaitStrategy getWaitStrategy() {
		return waitStrategy;
	}

	/**
	 * Set the {@link com.lmax.disruptor.WaitStrategy} to use when creating the internal {@link
	 * com.lmax.disruptor.RingBuffer}.
	 *
	 * @param waitStrategy
	 * 		the {@link com.lmax.disruptor.WaitStrategy}
	 *
	 * @return {@literal this}
	 */
	public WorkQueueAsyncTaskExecutor setWaitStrategy(WaitStrategy waitStrategy) {
		this.waitStrategy = waitStrategy;
		return this;
	}

	@Override
	protected AbstractLifecycleDispatcher getDispatcher() {
		return workQueue;
	}

}

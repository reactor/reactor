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
import reactor.event.dispatch.RingBufferDispatcher;
import reactor.function.Consumer;
import reactor.util.Assert;

/**
 * Implementation of {@link org.springframework.core.task.AsyncTaskExecutor} that uses a {@link RingBufferDispatcher}
 * to
 * execute tasks.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class RingBufferAsyncTaskExecutor extends AbstractAsyncTaskExecutor implements ApplicationEventPublisherAware {

	private final Logger log = LoggerFactory.getLogger(RingBufferAsyncTaskExecutor.class);

	private ProducerType              producerType;
	private WaitStrategy              waitStrategy;
	private ApplicationEventPublisher eventPublisher;
	private RingBufferDispatcher      dispatcher;

	public RingBufferAsyncTaskExecutor(Environment env) {
		super(env);
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher eventPublisher) {
		this.eventPublisher = eventPublisher;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.dispatcher = new RingBufferDispatcher(
				getName(),
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
	public RingBufferAsyncTaskExecutor setName(String name) {
		return (RingBufferAsyncTaskExecutor)super.setName(name);
	}

	@Override
	public int getThreads() {
		// RingBufferDispatchers are always single-threaded
		return 1;
	}

	@Override
	public RingBufferAsyncTaskExecutor setThreads(int threads) {
		Assert.isTrue(threads == 1, "A RingBufferAsyncTaskExecutor is always single-threaded");
		log.warn("RingBufferAsyncTaskExecutors are always single-threaded. Ignoring request to use " +
				         threads +
				         " threads.");
		return this;
	}

	@Override
	public RingBufferAsyncTaskExecutor setBacklog(int backlog) {
		return (RingBufferAsyncTaskExecutor)super.setBacklog(backlog);
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
	public RingBufferAsyncTaskExecutor setProducerType(ProducerType producerType) {
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
	public RingBufferAsyncTaskExecutor setWaitStrategy(WaitStrategy waitStrategy) {
		this.waitStrategy = waitStrategy;
		return this;
	}

	@Override
	protected AbstractLifecycleDispatcher getDispatcher() {
		return dispatcher;
	}

}

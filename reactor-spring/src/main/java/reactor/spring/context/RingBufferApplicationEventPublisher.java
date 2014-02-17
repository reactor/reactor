package reactor.spring.context;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.*;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 */
public class RingBufferApplicationEventPublisher implements ApplicationEventPublisher,
                                                            ApplicationContextAware,
                                                            SmartLifecycle {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final boolean                       autoStartup;
	private final ExecutorService               executor;
	private final Disruptor<AppEventSlot>       disruptor;
	private final EventTranslator<AppEventSlot> translator;

	private volatile boolean running = false;

	private RingBuffer<AppEventSlot> ringBuffer;
	private ApplicationContext       appCtx;

	public RingBufferApplicationEventPublisher(int backlog, boolean autoStartup) {
		this(backlog, autoStartup, ProducerType.MULTI, new BlockingWaitStrategy());
	}

	public RingBufferApplicationEventPublisher(int backlog,
	                                           boolean autoStartup,
	                                           ProducerType producerType,
	                                           WaitStrategy waitStrategy) {
		this.autoStartup = autoStartup;

		this.executor = new ThreadPoolExecutor(
				1,
				1,
				0,
				TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(),
				new NamedDaemonThreadFactory("ringBufferAppEventPublisher")
		);

		this.disruptor = new Disruptor<AppEventSlot>(
				new EventFactory<AppEventSlot>() {
					@Override
					public AppEventSlot newInstance() {
						return new AppEventSlot();
					}
				},
				backlog,
				executor,
				producerType,
				waitStrategy
		);

		this.disruptor.handleExceptionsWith(new ExceptionHandler() {
			@Override
			public void handleEventException(Throwable ex, long sequence, Object event) {
				log.error(ex.getMessage(), ex);
			}

			@Override
			public void handleOnStartException(Throwable ex) {
				log.error(ex.getMessage(), ex);
			}

			@Override
			public void handleOnShutdownException(Throwable ex) {
				log.error(ex.getMessage(), ex);
			}
		});

		this.translator = new EventTranslator<AppEventSlot>() {
			@Override
			public void translateTo(AppEventSlot event, long sequence) {
				appCtx.publishEvent(event.appEvent);
			}
		};

		if(autoStartup) {
			start();
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext appCtx) throws BeansException {
		this.appCtx = appCtx;
	}

	@Override
	public boolean isAutoStartup() {
		return autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		executor.shutdown();
		disruptor.shutdown();
		if(null != callback) {
			callback.run();
		}
		synchronized(this) {
			running = false;
		}
	}

	@Override
	public void start() {
		synchronized(this) {
			ringBuffer = disruptor.start();
			running = true;
		}
	}

	@Override
	public void stop() {
		stop(null);
	}

	@Override
	public boolean isRunning() {
		synchronized(this) {
			return running;
		}
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public void publishEvent(ApplicationEvent event) {
		ringBuffer.publishEvent(translator);
	}

	private static class AppEventSlot {
		ApplicationEvent appEvent;
	}

}

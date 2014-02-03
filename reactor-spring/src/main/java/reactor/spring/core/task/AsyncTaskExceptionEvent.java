package reactor.spring.core.task;

import org.springframework.context.ApplicationEvent;

/**
 * {@link org.springframework.context.ApplicationEvent} for publishing uncaught errors from asynchronous task execution.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class AsyncTaskExceptionEvent extends ApplicationEvent {
	public AsyncTaskExceptionEvent(Object source) {
		super(source);
	}
}

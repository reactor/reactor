package reactor.spring.core.task;

import org.springframework.context.ApplicationEvent;

/**
 * {@link org.springframework.context.ApplicationEvent} for publishing uncaught errors from asynchronous task execution.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class AsyncTaskExceptionEvent extends ApplicationEvent {

	private static final long serialVersionUID = 5172014386416785095L;

	public AsyncTaskExceptionEvent(Object source) {
		super(source);
	}
}

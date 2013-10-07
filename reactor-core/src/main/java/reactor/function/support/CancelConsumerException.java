package reactor.function.support;

/**
 * @author Jon Brisbin
 */
public class CancelConsumerException extends RuntimeException {
	private static final long serialVersionUID = 5373523865364055930L;

	public CancelConsumerException() {
	}

	public CancelConsumerException(String message) {
		super(message);
	}

}

package reactor.function.support;

/**
 * @author Jon Brisbin
 */
public class CancelConsumerException extends RuntimeException {

	public CancelConsumerException() {
	}

	public CancelConsumerException(String message) {
		super(message);
	}

}

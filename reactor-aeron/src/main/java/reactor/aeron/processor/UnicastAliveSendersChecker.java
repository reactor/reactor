package reactor.aeron.processor;

/**
 * A dummy implementation used in a unicast communication
 * between an instance of {@link AeronSubscriber} and {@link AeronPublisher}
 *
 * @author Anatoly Kadyshev
 */
public class UnicastAliveSendersChecker implements AliveSendersChecker {

	@Override
	public void scheduleCheckForAliveSenders() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public boolean isNoSendersDetected() {
		return false;
	}

}

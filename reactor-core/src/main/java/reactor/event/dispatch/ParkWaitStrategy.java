package reactor.event.dispatch;

import com.lmax.disruptor.*;

import java.util.concurrent.locks.LockSupport;

/**
 * @author Jon Brisbin
 */
public class ParkWaitStrategy implements WaitStrategy {

	private final long parkFor;

	public ParkWaitStrategy() {
		this(250);
	}

	public ParkWaitStrategy(long parkFor) {
		this.parkFor = parkFor;
	}

	@Override
	public long waitFor(long sequence,
	                    Sequence cursor,
	                    Sequence dependentSequence,
	                    SequenceBarrier barrier) throws AlertException,
	                                                    InterruptedException,
	                                                    TimeoutException {
		long availableSequence;
		while ((availableSequence = dependentSequence.get()) < sequence) {
			barrier.checkAlert();
			LockSupport.parkNanos(parkFor);
		}
		return availableSequence;
	}

	@Override
	public void signalAllWhenBlocking() {
	}

}

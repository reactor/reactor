package reactor.event.dispatch;

import jsr166e.ConcurrentHashMapV8;
import reactor.alloc.factory.BatchFactorySupplier;
import reactor.function.Supplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base implementation for multi-threaded dispatchers
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class MultiThreadDispatcher extends AbstractLifecycleDispatcher {

	private final int                                   backlog;
	private final int                                   numberThreads;
	private final int[]                                 tailRecurseSeqArray;
	private final int[]                                 tailRecursionPileSizeArray;
	private final List<List<Task>>                      tailRecursionPileList;
	private final BatchFactorySupplier<MultiThreadTask> taskFactory;

	private final Map<Long, Integer> tailsThreadIndexLookup = new ConcurrentHashMapV8<Long, Integer>();
	private final AtomicInteger      indexAssignPile        = new AtomicInteger();

	protected MultiThreadDispatcher(int numberThreads, int backlog) {
		this.backlog = backlog;
		this.numberThreads = numberThreads;
		this.tailRecurseSeqArray = new int[numberThreads];
		this.tailRecursionPileSizeArray = new int[numberThreads];
		this.tailRecursionPileList = new ArrayList<List<Task>>(numberThreads);
		this.taskFactory = new BatchFactorySupplier<MultiThreadTask>(
				backlog,
				new Supplier<MultiThreadTask>() {
					@Override
					public MultiThreadTask get() {
						return new MultiThreadTask();
					}
				}
		);

		for (int i = 0; i < numberThreads; i++) {
			tailRecursionPileSizeArray[i] = 0;
			tailRecurseSeqArray[i] = -1;
			tailRecursionPileList.add(new ArrayList<Task>(backlog));
			tailsThreadIndexLookup.put(Thread.currentThread().getId(), 0);
			expandTailRecursionPile(i, backlog);
		}
	}

	@Override
	public boolean supportsOrdering() {
		return false;
	}

	@Override
	public int backlogSize() {
		return backlog;
	}

	public int getNumberThreads() {
		return numberThreads;
	}

	public Integer getThreadIndex(){
		Integer index = tailsThreadIndexLookup.get(Thread.currentThread().getId());
		if(index == null) {
			index = indexAssignPile.getAndIncrement() % numberThreads;
			tailsThreadIndexLookup.put(Thread.currentThread().getId(), index);
		}
		return index;
	}

	protected void expandTailRecursionPile(int index, int amount) {
		List<Task> tailRecursionPile = tailRecursionPileList.get(index);
		for (int i = 0; i < amount; i++) {
			tailRecursionPile.add(new MultiThreadTask());
		}
		this.tailRecursionPileSizeArray[index] = tailRecursionPile.size();
	}

	@Override
	protected Task allocateRecursiveTask() {
		Integer index = getThreadIndex();
		int next = ++tailRecurseSeqArray[index];
		if (next == tailRecursionPileSizeArray[index]) {
			expandTailRecursionPile(index, backlog);
		}
		return tailRecursionPileList.get(index).get(next);
	}

	protected Task allocateTask() {
		return taskFactory.get();
	}

	protected class MultiThreadTask extends Task {
		@Override
		public void run() {
			route(this);

			Integer index = tailsThreadIndexLookup.get(Thread.currentThread().getId());
			if (null == index || tailRecurseSeqArray[index] < 0) {
				return;
			}
			//Process any recursive tasks
			Task task;
			List<Task> tailRecursePile = tailRecursionPileList.get(index);
			int next = 0;
			while (next <= tailRecurseSeqArray[index]) {
				task = tailRecursePile.get(next++);
				route(task);
			}
			// clean up extra tasks
			next = tailRecurseSeqArray[index];
			while (next > backlog) {
				tailRecursePile.remove(next--);
			}
			tailRecurseSeqArray[index] = -1;
		}
	}

}

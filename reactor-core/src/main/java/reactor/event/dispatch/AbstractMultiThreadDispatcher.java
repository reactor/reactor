package reactor.event.dispatch;

import com.gs.collections.api.block.function.Function0;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import reactor.alloc.factory.BatchFactorySupplier;
import reactor.function.Supplier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base implementation for multi-threaded dispatchers
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class AbstractMultiThreadDispatcher extends AbstractLifecycleDispatcher {

	private final int                                   backlog;
	private final int                                   numberThreads;
	private final int[]                                 tailRecurseSeqArray;
	private final int[]                                 tailRecursionPileSizeArray;
	private final List<List<Task>>                      tailRecursionPileList;
	private final BatchFactorySupplier<MultiThreadTask> taskFactory;

	private final UnifiedMap<Long, Integer> tailsThreadIndexLookup = UnifiedMap.newMap();
	private final AtomicInteger             indexAssignPile        = new AtomicInteger();

	protected AbstractMultiThreadDispatcher(int numberThreads, int backlog) {
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
			expandTailRecursionPile(i, backlog);
		}
		tailsThreadIndexLookup.put(Thread.currentThread().getId(), 0);
	}

	public int getBacklog() {
		return backlog;
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
		Long threadId = Thread.currentThread().getId();
		Integer index = tailsThreadIndexLookup.get(threadId);
		if (null == index) {
			index = tailsThreadIndexLookup.getIfAbsentPut(threadId, new Function0<Integer>() {
				@Override
				public Integer value() {
					return indexAssignPile.getAndIncrement() % numberThreads;
				}
			});
		}
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

package reactor.event.dispatch;

import reactor.queue.BlockingQueueFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base implementation for multithreaded dispatchers
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class AbstractMultiThreadDispatcher extends AbstractLifecycleDispatcher {

	private final BlockingQueue<Task> tasks;
	private final int                 backlog;
	private final int                 numberThreads;
	private final int[]               tailRecurseSeqArray;
	private final int[]               tailRecursionPileSizeArray;
	private final List<List<Task>>    tailRecursionPileList;

	private final Map<Integer, Integer> tailsThreadIndexLookup = new HashMap<Integer, Integer>();
	private final AtomicInteger         indexAssignPile        = new AtomicInteger();

	protected AbstractMultiThreadDispatcher(int numberThreads, int backlog) {
		this.backlog = backlog;
		this.numberThreads = numberThreads;
		this.tailRecurseSeqArray = new int[numberThreads];
		this.tailRecursionPileSizeArray = new int[numberThreads];
		this.tailRecursionPileList = new ArrayList<List<Task>>(numberThreads);

		this.tasks = BlockingQueueFactory.createQueue();

		for (int i = 0; i < backlog; i++) {
			tasks.add(new MultiThreadTask());
		}

		for (int i = 0; i < numberThreads; i++) {
			tailRecursionPileSizeArray[i] = 0;
			tailRecurseSeqArray[i] = -1;
			tailRecursionPileList.add(new ArrayList<Task>(backlog));
			tailsThreadIndexLookup.put(i, null);
			expandTailRecursionPile(i, backlog);
		}
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
		int hash = Thread.currentThread().hashCode();
		Integer index = tailsThreadIndexLookup.get(hash);
		if (null == index){
			index = indexAssignPile.getAndIncrement() % numberThreads;
			tailsThreadIndexLookup.put(hash, index);
		}
		int next = ++tailRecurseSeqArray[index];
		if (next == tailRecursionPileSizeArray[index]) {
			expandTailRecursionPile(index, backlog);
		}
		return tailRecursionPileList.get(index).get(next);
	}



	protected Task allocateTask() {
		Task task = tasks.poll();
		if (null != task) {
			return task;
		} else {
			return new MultiThreadTask();
		}
	}

	protected class MultiThreadTask extends Task {
		@Override
		public void run() {
			route(this);

			Integer index = tailsThreadIndexLookup.get(Thread.currentThread().hashCode());

			if(null == index)
				return;

			//Process any recursive tasks
			if(tailRecurseSeqArray[index] < 0) {
				return;
			}

			Task task;
			int next = 0;
			while(next <= tailRecurseSeqArray[index]) {
				task = tailRecursionPileList.get(index).get(next++);
				route(task);
			}
			// clean up extra tasks
			next = tailRecurseSeqArray[index];
			while(next > backlog) {
				tailRecursionPileList.get(index).remove(next--);
			}
			tailRecurseSeqArray[index] = -1;
			tasks.offer(this);
		}
	}

}

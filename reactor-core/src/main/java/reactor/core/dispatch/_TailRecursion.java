package reactor.core.dispatch;

import reactor.fn.Supplier;

import java.util.ArrayList;

/**
 * @author Anatoly Kadyshev
 */
class _TailRecursion {

    private final ArrayList<RingBufferDispatcher3.Task> pile;

    private final int backlogSize;

    private final Supplier<RingBufferDispatcher3.Task> taskSupplier;

    private int cursor = -1;

    _TailRecursion(int backlogSize, Supplier<RingBufferDispatcher3.Task> taskSupplier) {
        this.backlogSize = backlogSize;
        this.taskSupplier = taskSupplier;
        this.pile = new ArrayList<RingBufferDispatcher3.Task>(backlogSize);
        fillInPile(backlogSize);
    }

    private void fillInPile(int n) {
        for (int i = 0; i < n; i++) {
            pile.add(taskSupplier.get());
        }
    }

    public RingBufferDispatcher3.Task next() {
        cursor++;
        if (cursor >= pile.size()) {
            pile.ensureCapacity(pile.size() + backlogSize);
            fillInPile(backlogSize);
        }
        return pile.get(cursor);
    }

    public int getCursor() {
        return cursor;
    }

    public RingBufferDispatcher3.Task get(int i) {
        return pile.get(i);
    }

    public void reset() {
        cursor = -1;
    }
}

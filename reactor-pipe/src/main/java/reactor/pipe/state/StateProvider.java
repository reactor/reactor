package reactor.pipe.state;

import reactor.pipe.concurrent.Atom;

/**
 * Combined with `Atom`, State Providers are the flexible way to hold
 * state in memory in case the stream processor crashes or has to be
 * restarted.
 *
 * @param <K> Type of the key.
 */
public interface StateProvider<K> {

    /**
     * Initializes the state for the given key. State might be taken from the database,
     * or calculated based on some sort of rule. If there's no state known for the key
     * {@code init} initial state should be used.
     *
     * @param src key for which state should be retrieved / constructed.
     * @param init initial "default" state for the current key
     * @param <T> type of the key.
     * @return
     */
    public <T> Atom<T> makeAtom(K src, T init);
}

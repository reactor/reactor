package reactor.core.processor;

/**
 * @author Anatoly Kadyshev
 */
public interface ImmutableSignal<T> {

    T getValue();

    long getSeqId();

}

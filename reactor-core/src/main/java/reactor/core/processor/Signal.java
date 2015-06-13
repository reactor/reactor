package reactor.core.processor;

/**
 * @author Anatoly Kadyshev
 */
public interface Signal<T> {

    T getValue();

    long getSeqId();

}

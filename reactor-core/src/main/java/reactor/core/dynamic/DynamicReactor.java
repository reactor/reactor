package reactor.core.dynamic;

/**
 * A {@literal DynamicReactor} is an arbitrary interface that a proxy generator can use to wire calls to the interface
 * to appropriate {@link reactor.core.Reactor#on(reactor.fn.Selector, reactor.fn.Consumer)} and {@link
 * reactor.core.Reactor#notify(Object, reactor.fn.Event)} calls.
 *
 * @author Jon Brisbin
 */
public interface DynamicReactor {
}

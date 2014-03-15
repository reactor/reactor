package reactor.event.selector;

/**
 * Implementation of {@link reactor.event.selector.Selector} that matches
 * all objects.
 *
 * @author Michael Klishin
 */
public class MatchAllSelector implements Selector {

    @Override
    public Object getObject() {
        return null;
    }

    @Override
    public boolean matches(Object key) {
        return true;
    }

    @Override
    public HeaderResolver getHeaderResolver() {
        return null;
    }
}

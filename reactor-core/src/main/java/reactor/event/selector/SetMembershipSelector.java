package reactor.event.selector;

import java.util.Set;

/**
 * Implementation of {@link reactor.event.selector.Selector} that matches
 * objects on set membership.
 *
 * @author Michael Klishin
 */
public class SetMembershipSelector implements Selector {
	private final Set set;

	/**
	 * Create a {@link Selector} when the given regex pattern.
	 *
	 * @param set
	 * 		The {@link Set} that will be used for membership checks.
	 */
	public SetMembershipSelector(Set set) {
		this.set = set;
	}

	@Override
	public Object getObject() {
		return this.set;
	}

	@Override
	public boolean matches(Object key) {
		return this.set.contains(key);
	}

	@Override
	public HeaderResolver getHeaderResolver() {
		return null;
	}
}

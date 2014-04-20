package reactor.event.selector;

import java.util.Set;

public class SetSelector implements Selector {
	private final Set set;

	public SetSelector(Set set) {
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

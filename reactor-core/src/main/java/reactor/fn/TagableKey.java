package reactor.fn;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * A helper class to make a notification key {@link Tagable}.
 *
 * @author Andy Wilkinson
 */
public final class TagableKey implements Tagable<TagableKey> {

	private final Object delegate;

	private Set<String> tags;

	/**
	 * Creates a {@link Tagable} notification key that will defer to the
	 * {@code delegate} for {@link hashCode} and {@link equals}.
	 *
	 * @param delegate The delegate
	 */
	public TagableKey(Object delegate) {
		this.delegate = delegate;
	}

	@Override
	public Tagable<TagableKey> setTags(String... tags) {
		if (null == this.tags) {
			this.tags = new TreeSet<String>();
		} else {
			this.tags.clear();
		}
		Collections.addAll(this.tags, tags);
		return this;
	}

	@Override
	public Set<String> getTags() {
		return (null == tags ? Collections.<String>emptySet() : tags);
	}

	@Override
	public Object getTagged() {
		return delegate;
	}

	@Override
	public boolean equals(Object other) {
		return this.delegate.equals(other);
	}

	@Override
	public int hashCode() {
		return this.delegate.hashCode();
	}
}

package reactor.fn;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * A helper class to make a notification key {@link Taggable}.
 *
 * @author Andy Wilkinson
 */
public final class TaggableKey implements Taggable<TaggableKey> {

	private final Object delegate;

	private Set<String> tags;

	/**
	 * Creates a {@link Taggable} notification key that will defer to the
	 * {@code delegate} for {@link #hashCode} and {@link #equals}.
	 *
	 * @param delegate The delegate
	 */
	public TaggableKey(Object delegate) {
		this.delegate = delegate;
	}

	@Override
	public Taggable<TaggableKey> setTags(String... tags) {
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

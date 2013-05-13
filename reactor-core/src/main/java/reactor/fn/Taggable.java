package reactor.fn;

import java.util.Set;

/**
 * A {@code Taggable} object maintains a set of tags
 *
 * @author Andy Wilkinson
 *
 * @param <T> The tagable type subclass
 */
public interface Taggable<T extends Taggable<T>> {

	/**
	 * Set the set of tags. Wipes out any currently assigned tags.
	 *
	 * @param tags The full set of tags to assign
	 * @return {@literal this}
	 */
	Taggable<T> setTags(String... tags);

	/**
	 * Get the set of tags currently assigned to this {@literal Selector}.
	 *
	 * @return the tags
	 */
	Set<String> getTags();

	/**
	 * Returns the object that is tagged
	 *
	 * @return the tagged object
	 */
	Object getTagged();

}

package io.spring.reactor.annotation;

/**
 * Enum for indicating the type of {@link reactor.event.selector.Selector} that should be created.
 *
 * @author Jon Brisbin
 */
public enum SelectorType {
	OBJECT, REGEX, URI, TYPE, JSON_PATH
}

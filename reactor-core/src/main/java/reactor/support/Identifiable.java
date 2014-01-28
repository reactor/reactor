package reactor.support;

/**
 * @author Jon Brisbin
 */
public interface Identifiable<ID> {

	Identifiable<ID> setId(ID id);

	ID getId();

}

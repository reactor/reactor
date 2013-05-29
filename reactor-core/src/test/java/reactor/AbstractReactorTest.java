package reactor;

import org.junit.Before;
import reactor.core.Environment;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractReactorTest {

	protected Environment env;

	@Before
	public void loadEnv() {
		env = new Environment();
	}

}

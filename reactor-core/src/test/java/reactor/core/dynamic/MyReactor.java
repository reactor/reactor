package reactor.core.dynamic;

import reactor.core.dynamic.annotation.Dispatcher;
import reactor.core.dynamic.annotation.DispatcherType;
import reactor.core.dynamic.annotation.On;
import reactor.fn.Consumer;

/**
 * @author Jon Brisbin
 */
@Dispatcher(DispatcherType.THREAD_POOL)
public interface MyReactor extends DynamicReactor {

	@On("test.test")
	MyReactor onTestTest(Consumer<String> s);

	MyReactor onTest(Consumer<String> s);

	MyReactor notifyTest(String s);

	MyReactor notifyTestTest(String s);

}

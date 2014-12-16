package reactor.groovy.ext

import groovy.transform.CompileStatic
import reactor.core.dispatch.processor.spec.ProcessorSpec
import reactor.groovy.support.ClosureConsumer
import reactor.groovy.support.ClosureSupplier

/**
 * Groovy extension module for {@link ProcessorSpec}.
 *
 * @author Jon Brisbin
 */
@CompileStatic
class ProcessorExtensions {

  /**
   * Provide a {@link Closure} as a {@code dataSupplier}.
   *
   * @param selfType
   * @param closure
   * @return
   */
  static <T> ProcessorSpec<T> dataSupplier(ProcessorSpec<T> selfType, Closure<T> closure) {
    selfType.dataSupplier(new ClosureSupplier<T>(closure))
  }

  /**
   * Provide a {@link Closure} as a {@link reactor.fn.Consumer}.
   *
   * @param selfType
   * @param closure
   * @return
   */
  static <T> ProcessorSpec<T> consume(ProcessorSpec<T> selfType, Closure closure) {
    selfType.consume(new ClosureConsumer<T>(closure))
  }

  /**
   * Provide a {@link Closure} as an error {@link reactor.fn.Consumer}.
   *
   * @param selfType
   * @param closure
   * @return
   */
  static <T> ProcessorSpec<T> when(ProcessorSpec<T> selfType, Class<Throwable> type, Closure closure) {
    selfType.when(type, new ClosureConsumer<Throwable>(closure))
  }

}

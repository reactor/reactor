package reactor.groovy.ext

import groovy.transform.CompileStatic
import reactor.core.processor.Processor
import reactor.core.processor.spec.ProcessorSpec
import reactor.function.Consumer
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
   * Provide a {@link Closure} as a {@link reactor.function.Consumer}.
   *
   * @param selfType
   * @param closure
   * @return
   */
  static <T> Processor<T> consume(Processor<T> selfType, Closure... closures) {
    selfType.consume((Collection<Consumer<T>>) closures.collect { Closure cl -> new ClosureConsumer<T>(cl) })
  }

  /**
   * Provide a {@link Closure} as an error {@link reactor.function.Consumer}.
   *
   * @param selfType
   * @param closure
   * @return
   */
  static <T> Processor<T> when(Processor<T> selfType, Class<Throwable> type, Closure closure) {
    selfType.when(type, new ClosureConsumer<Throwable>(closure))
  }

}

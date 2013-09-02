package reactor.groovy.config

import groovy.transform.CompileStatic

import static groovy.lang.Closure.DELEGATE_FIRST

/**
 * Author: smaldini
 */
@CompileStatic
class DSLUtils {

	static public final Closure EMPTY_CLOSURE = {...args->}

	/**
	 * Helper for recurrent use-case : Delegating a closure to a builder and resolving it first
	 * @param builder
	 * @param closure
	 * @return possible closure result
	 */
	static <T> T delegateFirstAndRun(builder, Closure<T> closure){
		closure.delegate = builder
		closure.resolveStrategy = DELEGATE_FIRST
		closure()
	}

}
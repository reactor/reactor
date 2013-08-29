package reactor.groovy.config

import groovy.transform.CompileStatic

import static groovy.lang.Closure.DELEGATE_FIRST

/**
 * @author Stephane Maldini
 */
@CompileStatic
class ReactorScriptWrapper extends Script {

	GroovyEnvironment doWithReactor(@DelegatesTo(strategy = DELEGATE_FIRST, value = GroovyEnvironment) Closure c) {
		GroovyEnvironment.create c
	}

	@Override
	Object run() {
		super.run()
	}
}

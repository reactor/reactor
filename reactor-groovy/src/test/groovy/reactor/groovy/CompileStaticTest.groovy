package reactor.groovy

import groovy.transform.CompileStatic
import reactor.core.Environment
import reactor.core.R
import reactor.fn.Supplier

/**
 * This class shouldnt fail compilation
 *
 * @author Stephane Maldini
 */
@CompileStatic
class CompileStaticTest {

	Environment env = new Environment()

	def run() {

		def testClosure = {
			'test'
		}

		def supplier = new Supplier<String>(){
			@Override
			String get() {
				'test'
			}
		}

		R.task(supplier).using(env)
	}

	static void main(String[] args) {
		new CompileStaticTest().run()

	}
}

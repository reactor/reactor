package reactor.bus

import spock.lang.Specification

class HeadersSpec extends Specification {

	def 'Header names are case-insensitive'() {
		when: 'A header is named alpha'
		Event.Headers headers = new Event.Headers()
		headers.set('alpha', 'a')

		then: 'It can be retrieved using aLpHa'
		'a' == headers.get('aLpHa')
	}

	def 'Headers can be made read-only'() {
		given: 'Some read-only headers'
		Event.Headers headers = new Event.Headers().readOnly()

		when: 'They are modified'
		headers.set('a', 'alpha')

		then: 'Then it fails with an UnsupportedOperationException'
		thrown(UnsupportedOperationException)
	}

	def 'Headers can be created from a Map'() {
		given: 'A map'
		Map map = ['a':'alpha', 'b':'bravo']

		when: 'Headers are created from the map'
		Event.Headers headers = new Event.Headers(map)

		then: 'They contain the contents of the map'
		'alpha' == headers.get('a')
		'bravo' == headers.get('b')
	}

	def 'Headers can return its contents as an unmodifiable map'() {
		given: 'A Headers instance containing some headers'
		Event.Headers headers = new Event.Headers()
		headers.set('a', 'alpha')
		headers.set('b', 'bravo')

		when: 'A map is created from the Headers'
		Map map = headers.asMap()

		then: 'The map contains the headers'
		'alpha' == map.get('a')
		'bravo' == map.get('b')

		when: 'The map is modified'
		map.put('c', 'charlie')

		then: 'An UnsupportedOperationException is thrown'
		thrown(UnsupportedOperationException)
	}

	def 'Headers provides an iterator over its contents'() {
		given: 'A headers instance containing some headers'
		Event.Headers headers = new Event.Headers()
		headers.set('a', 'alpha')
		headers.set('b', 'bravo')
		Iterator iterator = headers.iterator()

		when: 'An item is removed using the iterator'
		iterator.remove()

		then: 'An UnsupportedOperationException is thrown'
		thrown(UnsupportedOperationException)

		when: 'The iterator is used to retrieve the headers'
		Map map = [:]
		iterator.each { header ->
			map.put(header.t1, header.t2)
		}

		then: 'The iterator provided access to the headers'
		map.size() == 2
		'alpha' == map.get('a')
		'bravo' == map.get('b')
	}

	def 'Setting a header with a null value removes the header'() {
		given: 'A Headers instance containing a header'
		Event.Headers headers = new Event.Headers()
		headers.set('a', 'alpha')

		when: "That header's value is set to null"
		headers.set('a', null)

		then: 'It is removed from the headers'
		!headers.contains('a')
	}

	def 'The origin can be set using a UUID'() {
		given: 'A Headers instance and a UUID'
		Event.Headers headers = new Event.Headers()
		UUID uuid = UUID.randomUUID()

		when: 'The origin is set with the UUID'
		headers.setOrigin(uuid)

		then: 'The origin is the string form of the UUID'
		uuid.toString() == headers.getOrigin()
	}

	def 'The origin can be set using a String'() {
		given: 'A Headers instance and an origin string'
		Event.Headers headers = new Event.Headers()
		String uuid = '1234567890'

		when: 'The origin is set'
		headers.setOrigin(uuid)

		then: 'The origin can be retrieved'
		uuid == headers.getOrigin()
	}

	def 'Setting the origin with a null UUID removes the origin'() {
		given: 'A Headers instance with an origin'
		Event.Headers headers = new Event.Headers()
		headers.setOrigin('1234567890')

		when: 'The origin is set to a null UUID'
		headers.setOrigin((UUID)null)

		then: 'The origin has been removed'
		headers.getOrigin() == null
	}

	def 'The contents of a Map can be added to a Headers instance'() {
		given: 'A Headers instance containg a header'
		Event.Headers headers = new Event.Headers()
		headers.set('a', 'alpha')
		headers.set('b', 'bravo')

		when: 'A map of headers is set'
		headers.setAll('a':'aardvark', 'b':null, 'c':'charlie')

		then: 'The headers have been updated'
		'aardvark' == headers.get('a')
		'charlie' == headers.get('c')
		!headers.contains('b')
	}

}

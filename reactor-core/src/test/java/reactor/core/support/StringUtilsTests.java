/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.support;

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * @author Rod Johnson
 * @author Juergen Hoeller
 * @author Rick Evans
 */
public class StringUtilsTests extends TestCase {

	public void testHasTextBlank() throws Exception {
		String blank = "          ";
		assertEquals(false, StringUtils.hasText(blank));
	}

	public void testHasTextNullEmpty() throws Exception {
		assertEquals(false, StringUtils.hasText(null));
		assertEquals(false, StringUtils.hasText(""));
	}

	public void testHasTextValid() throws Exception {
		assertEquals(true, StringUtils.hasText("t"));
	}

	public void testContainsWhitespace() throws Exception {
		org.junit.Assert.assertFalse(StringUtils.containsWhitespace(null));
		org.junit.Assert.assertFalse(StringUtils.containsWhitespace(""));
		org.junit.Assert.assertFalse(StringUtils.containsWhitespace("a"));
		org.junit.Assert.assertFalse(StringUtils.containsWhitespace("abc"));
		org.junit.Assert.assertTrue(StringUtils.containsWhitespace(" "));
		org.junit.Assert.assertTrue(StringUtils.containsWhitespace(" a"));
		org.junit.Assert.assertTrue(StringUtils.containsWhitespace("abc "));
		org.junit.Assert.assertTrue(StringUtils.containsWhitespace("a b"));
		org.junit.Assert.assertTrue(StringUtils.containsWhitespace("a  b"));
	}

	public void testTrimWhitespace() throws Exception {
		assertEquals(null, StringUtils.trimWhitespace(null));
		assertEquals("", StringUtils.trimWhitespace(""));
		assertEquals("", StringUtils.trimWhitespace(" "));
		assertEquals("", StringUtils.trimWhitespace("\t"));
		assertEquals("a", StringUtils.trimWhitespace(" a"));
		assertEquals("a", StringUtils.trimWhitespace("a "));
		assertEquals("a", StringUtils.trimWhitespace(" a "));
		assertEquals("a b", StringUtils.trimWhitespace(" a b "));
		assertEquals("a b  c", StringUtils.trimWhitespace(" a b  c "));
	}

	public void testTrimAllWhitespace() throws Exception {
		assertEquals("", StringUtils.trimAllWhitespace(""));
		assertEquals("", StringUtils.trimAllWhitespace(" "));
		assertEquals("", StringUtils.trimAllWhitespace("\t"));
		assertEquals("a", StringUtils.trimAllWhitespace(" a"));
		assertEquals("a", StringUtils.trimAllWhitespace("a "));
		assertEquals("a", StringUtils.trimAllWhitespace(" a "));
		assertEquals("ab", StringUtils.trimAllWhitespace(" a b "));
		assertEquals("abc", StringUtils.trimAllWhitespace(" a b  c "));
	}

	public void testTrimLeadingWhitespace() throws Exception {
		assertEquals(null, StringUtils.trimLeadingWhitespace(null));
		assertEquals("", StringUtils.trimLeadingWhitespace(""));
		assertEquals("", StringUtils.trimLeadingWhitespace(" "));
		assertEquals("", StringUtils.trimLeadingWhitespace("\t"));
		assertEquals("a", StringUtils.trimLeadingWhitespace(" a"));
		assertEquals("a ", StringUtils.trimLeadingWhitespace("a "));
		assertEquals("a ", StringUtils.trimLeadingWhitespace(" a "));
		assertEquals("a b ", StringUtils.trimLeadingWhitespace(" a b "));
		assertEquals("a b  c ", StringUtils.trimLeadingWhitespace(" a b  c "));
	}

	public void testTrimTrailingWhitespace() throws Exception {
		assertEquals(null, StringUtils.trimTrailingWhitespace(null));
		assertEquals("", StringUtils.trimTrailingWhitespace(""));
		assertEquals("", StringUtils.trimTrailingWhitespace(" "));
		assertEquals("", StringUtils.trimTrailingWhitespace("\t"));
		assertEquals("a", StringUtils.trimTrailingWhitespace("a "));
		assertEquals(" a", StringUtils.trimTrailingWhitespace(" a"));
		assertEquals(" a", StringUtils.trimTrailingWhitespace(" a "));
		assertEquals(" a b", StringUtils.trimTrailingWhitespace(" a b "));
		assertEquals(" a b  c", StringUtils.trimTrailingWhitespace(" a b  c "));
	}

	public void testTrimLeadingCharacter() throws Exception {
		assertEquals(null, StringUtils.trimLeadingCharacter(null, ' '));
		assertEquals("", StringUtils.trimLeadingCharacter("", ' '));
		assertEquals("", StringUtils.trimLeadingCharacter(" ", ' '));
		assertEquals("\t", StringUtils.trimLeadingCharacter("\t", ' '));
		assertEquals("a", StringUtils.trimLeadingCharacter(" a", ' '));
		assertEquals("a ", StringUtils.trimLeadingCharacter("a ", ' '));
		assertEquals("a ", StringUtils.trimLeadingCharacter(" a ", ' '));
		assertEquals("a b ", StringUtils.trimLeadingCharacter(" a b ", ' '));
		assertEquals("a b  c ", StringUtils.trimLeadingCharacter(" a b  c ", ' '));
	}

	public void testTrimTrailingCharacter() throws Exception {
		assertEquals(null, StringUtils.trimTrailingCharacter(null, ' '));
		assertEquals("", StringUtils.trimTrailingCharacter("", ' '));
		assertEquals("", StringUtils.trimTrailingCharacter(" ", ' '));
		assertEquals("\t", StringUtils.trimTrailingCharacter("\t", ' '));
		assertEquals("a", StringUtils.trimTrailingCharacter("a ", ' '));
		assertEquals(" a", StringUtils.trimTrailingCharacter(" a", ' '));
		assertEquals(" a", StringUtils.trimTrailingCharacter(" a ", ' '));
		assertEquals(" a b", StringUtils.trimTrailingCharacter(" a b ", ' '));
		assertEquals(" a b  c", StringUtils.trimTrailingCharacter(" a b  c ", ' '));
	}

	public void testCountOccurrencesOf() {
		org.junit.Assert.assertTrue("nullx2 = 0",
				StringUtils.countOccurrencesOf(null, null) == 0);
		org.junit.Assert.assertTrue("null string = 0",
				StringUtils.countOccurrencesOf("s", null) == 0);
		org.junit.Assert.assertTrue("null substring = 0",
				StringUtils.countOccurrencesOf(null, "s") == 0);
		String s = "erowoiueoiur";
		org.junit.Assert.assertTrue("not found = 0",
				StringUtils.countOccurrencesOf(s, "WERWER") == 0);
		org.junit.Assert.assertTrue("not found char = 0",
				StringUtils.countOccurrencesOf(s, "x") == 0);
		org.junit.Assert.assertTrue("not found ws = 0",
				StringUtils.countOccurrencesOf(s, " ") == 0);
		org.junit.Assert.assertTrue("not found empty string = 0",
				StringUtils.countOccurrencesOf(s, "") == 0);
		org.junit.Assert.assertTrue("found char=2", StringUtils.countOccurrencesOf(s, "e") == 2);
		org.junit.Assert.assertTrue("found substring=2",
				StringUtils.countOccurrencesOf(s, "oi") == 2);
		org.junit.Assert.assertTrue("found substring=2",
				StringUtils.countOccurrencesOf(s, "oiu") == 2);
		org.junit.Assert.assertTrue("found substring=3",
				StringUtils.countOccurrencesOf(s, "oiur") == 1);
		org.junit.Assert.assertTrue("test last", StringUtils.countOccurrencesOf(s, "r") == 2);
	}

	public void testReplace() throws Exception {
		String inString = "a6AazAaa77abaa";
		String oldPattern = "aa";
		String newPattern = "foo";

		// Simple replace
		String s = StringUtils.replace(inString, oldPattern, newPattern);
		org.junit.Assert.assertTrue("Replace 1 worked", s.equals("a6AazAfoo77abfoo"));

		// Non match: no change
		s = StringUtils.replace(inString, "qwoeiruqopwieurpoqwieur", newPattern);
		org.junit.Assert.assertTrue("Replace non matched is equal", s.equals(inString));

		// Null new pattern: should ignore
		s = StringUtils.replace(inString, oldPattern, null);
		org.junit.Assert.assertTrue("Replace non matched is equal", s.equals(inString));

		// Null old pattern: should ignore
		s = StringUtils.replace(inString, null, newPattern);
		org.junit.Assert.assertTrue("Replace non matched is equal", s.equals(inString));
	}

	public void testDelete() throws Exception {
		String inString = "The quick brown fox jumped over the lazy dog";

		String noThe = StringUtils.delete(inString, "the");
		org.junit.Assert.assertTrue("Result has no the [" + noThe + "]",
				noThe.equals("The quick brown fox jumped over  lazy dog"));

		String nohe = StringUtils.delete(inString, "he");
		org.junit.Assert.assertTrue("Result has no he [" + nohe + "]",
				nohe.equals("T quick brown fox jumped over t lazy dog"));

		String nosp = StringUtils.delete(inString, " ");
		org.junit.Assert.assertTrue("Result has no spaces",
				nosp.equals("Thequickbrownfoxjumpedoverthelazydog"));

		String killEnd = StringUtils.delete(inString, "dog");
		org.junit.Assert.assertTrue("Result has no dog",
				killEnd.equals("The quick brown fox jumped over the lazy "));

		String mismatch = StringUtils.delete(inString, "dxxcxcxog");
		org.junit.Assert.assertTrue("Result is unchanged", mismatch.equals(inString));

		String nochange = StringUtils.delete(inString, "");
		org.junit.Assert.assertTrue("Result is unchanged", nochange.equals(inString));
	}

	public void testDeleteAny() throws Exception {
		String inString = "Able was I ere I saw Elba";

		String res = StringUtils.deleteAny(inString, "I");
		org.junit.Assert.assertTrue("Result has no Is [" + res + "]", res.equals("Able was  ere  saw Elba"));

		res = StringUtils.deleteAny(inString, "AeEba!");
		org.junit.Assert.assertTrue("Result has no Is [" + res + "]", res.equals("l ws I r I sw l"));

		String mismatch = StringUtils.deleteAny(inString, "#@$#$^");
		org.junit.Assert.assertTrue("Result is unchanged", mismatch.equals(inString));

		String whitespace = "This is\n\n\n    \t   a messagy string with whitespace\n";
		org.junit.Assert.assertTrue("Has CR", whitespace.indexOf("\n") != -1);
		org.junit.Assert.assertTrue("Has tab", whitespace.indexOf("\t") != -1);
		org.junit.Assert.assertTrue("Has  sp", whitespace.indexOf(" ") != -1);
		String cleaned = StringUtils.deleteAny(whitespace, "\n\t ");
		org.junit.Assert.assertTrue("Has no CR", cleaned.indexOf("\n") == -1);
		org.junit.Assert.assertTrue("Has no tab", cleaned.indexOf("\t") == -1);
		org.junit.Assert.assertTrue("Has no sp", cleaned.indexOf(" ") == -1);
		org.junit.Assert.assertTrue("Still has chars", cleaned.length() > 10);
	}


	public void testQuote() {
		assertEquals("'myString'", StringUtils.quote("myString"));
		assertEquals("''", StringUtils.quote(""));
		org.junit.Assert.assertNull(StringUtils.quote(null));
	}

	public void testQuoteIfString() {
		assertEquals("'myString'", StringUtils.quoteIfString("myString"));
		assertEquals("''", StringUtils.quoteIfString(""));
		assertEquals(new Integer(5), StringUtils.quoteIfString(new Integer(5)));
		org.junit.Assert.assertNull(StringUtils.quoteIfString(null));
	}

	public void testUnqualify() {
		String qualified = "i.am.not.unqualified";
		assertEquals("unqualified", StringUtils.unqualify(qualified));
	}

	public void testCapitalize() {
		String capitalized = "i am not capitalized";
		assertEquals("I am not capitalized", StringUtils.capitalize(capitalized));
	}

	public void testUncapitalize() {
		String capitalized = "I am capitalized";
		assertEquals("i am capitalized", StringUtils.uncapitalize(capitalized));
	}

	public void testGetFilename() {
		assertEquals(null, StringUtils.getFilename(null));
		assertEquals("", StringUtils.getFilename(""));
		assertEquals("myfile", StringUtils.getFilename("myfile"));
		assertEquals("myfile", StringUtils.getFilename("mypath/myfile"));
		assertEquals("myfile.", StringUtils.getFilename("myfile."));
		assertEquals("myfile.", StringUtils.getFilename("mypath/myfile."));
		assertEquals("myfile.txt", StringUtils.getFilename("myfile.txt"));
		assertEquals("myfile.txt", StringUtils.getFilename("mypath/myfile.txt"));
	}

	public void testGetFilenameExtension() {
		assertEquals(null, StringUtils.getFilenameExtension(null));
		assertEquals(null, StringUtils.getFilenameExtension(""));
		assertEquals(null, StringUtils.getFilenameExtension("myfile"));
		assertEquals(null, StringUtils.getFilenameExtension("myPath/myfile"));
		assertEquals(null, StringUtils.getFilenameExtension("/home/user/.m2/settings/myfile"));
		assertEquals("", StringUtils.getFilenameExtension("myfile."));
		assertEquals("", StringUtils.getFilenameExtension("myPath/myfile."));
		assertEquals("txt", StringUtils.getFilenameExtension("myfile.txt"));
		assertEquals("txt", StringUtils.getFilenameExtension("mypath/myfile.txt"));
		assertEquals("txt", StringUtils.getFilenameExtension("/home/user/.m2/settings/myfile.txt"));
	}

	public void testStripFilenameExtension() {
		assertEquals(null, StringUtils.stripFilenameExtension(null));
		assertEquals("", StringUtils.stripFilenameExtension(""));
		assertEquals("myfile", StringUtils.stripFilenameExtension("myfile"));
		assertEquals("myfile", StringUtils.stripFilenameExtension("myfile."));
		assertEquals("myfile", StringUtils.stripFilenameExtension("myfile.txt"));
		assertEquals("mypath/myfile", StringUtils.stripFilenameExtension("mypath/myfile"));
		assertEquals("mypath/myfile", StringUtils.stripFilenameExtension("mypath/myfile."));
		assertEquals("mypath/myfile", StringUtils.stripFilenameExtension("mypath/myfile.txt"));
		assertEquals("/home/user/.m2/settings/myfile", StringUtils.stripFilenameExtension("/home/user/.m2/settings/myfile"));
		assertEquals("/home/user/.m2/settings/myfile", StringUtils.stripFilenameExtension("/home/user/.m2/settings/myfile."));
		assertEquals("/home/user/.m2/settings/myfile", StringUtils.stripFilenameExtension("/home/user/.m2/settings/myfile.txt"));
	}

	public void testCleanPath() {
		assertEquals("mypath/myfile", StringUtils.cleanPath("mypath/myfile"));
		assertEquals("mypath/myfile", StringUtils.cleanPath("mypath\\myfile"));
		assertEquals("mypath/myfile", StringUtils.cleanPath("mypath/../mypath/myfile"));
		assertEquals("mypath/myfile", StringUtils.cleanPath("mypath/myfile/../../mypath/myfile"));
		assertEquals("../mypath/myfile", StringUtils.cleanPath("../mypath/myfile"));
		assertEquals("../mypath/myfile", StringUtils.cleanPath("../mypath/../mypath/myfile"));
		assertEquals("../mypath/myfile", StringUtils.cleanPath("mypath/../../mypath/myfile"));
		assertEquals("/../mypath/myfile", StringUtils.cleanPath("/../mypath/myfile"));
	}

	public void testPathEquals() {
		org.junit.Assert.assertTrue("Must be true for the same strings",
				StringUtils.pathEquals("/dummy1/dummy2/dummy3",
						"/dummy1/dummy2/dummy3"));
		org.junit.Assert.assertTrue("Must be true for the same win strings",
				StringUtils.pathEquals("C:\\dummy1\\dummy2\\dummy3",
						"C:\\dummy1\\dummy2\\dummy3"));
		org.junit.Assert.assertTrue("Must be true for one top path on 1",
				StringUtils.pathEquals("/dummy1/bin/../dummy2/dummy3",
						"/dummy1/dummy2/dummy3"));
		org.junit.Assert.assertTrue("Must be true for one win top path on 2",
				StringUtils.pathEquals("C:\\dummy1\\dummy2\\dummy3",
						"C:\\dummy1\\bin\\..\\dummy2\\dummy3"));
		org.junit.Assert.assertTrue("Must be true for two top paths on 1",
				StringUtils.pathEquals("/dummy1/bin/../dummy2/bin/../dummy3",
						"/dummy1/dummy2/dummy3"));
		org.junit.Assert.assertTrue("Must be true for two win top paths on 2",
				StringUtils.pathEquals("C:\\dummy1\\dummy2\\dummy3",
						"C:\\dummy1\\bin\\..\\dummy2\\bin\\..\\dummy3"));
		org.junit.Assert.assertTrue("Must be true for double top paths on 1",
				StringUtils.pathEquals("/dummy1/bin/tmp/../../dummy2/dummy3",
						"/dummy1/dummy2/dummy3"));
		org.junit.Assert.assertTrue("Must be true for double top paths on 2 with similarity",
				StringUtils.pathEquals("/dummy1/dummy2/dummy3",
						"/dummy1/dum/dum/../../dummy2/dummy3"));
		org.junit.Assert.assertTrue("Must be true for current paths",
				StringUtils.pathEquals("./dummy1/dummy2/dummy3",
						"dummy1/dum/./dum/../../dummy2/dummy3"));
		org.junit.Assert.assertFalse("Must be false for relative/absolute paths",
				StringUtils.pathEquals("./dummy1/dummy2/dummy3",
						"/dummy1/dum/./dum/../../dummy2/dummy3"));
		org.junit.Assert.assertFalse("Must be false for different strings",
				StringUtils.pathEquals("/dummy1/dummy2/dummy3",
						"/dummy1/dummy4/dummy3"));
		org.junit.Assert.assertFalse("Must be false for one false path on 1",
				StringUtils.pathEquals("/dummy1/bin/tmp/../dummy2/dummy3",
						"/dummy1/dummy2/dummy3"));
		org.junit.Assert.assertFalse("Must be false for one false win top path on 2",
				StringUtils.pathEquals("C:\\dummy1\\dummy2\\dummy3",
						"C:\\dummy1\\bin\\tmp\\..\\dummy2\\dummy3"));
		org.junit.Assert.assertFalse("Must be false for top path on 1 + difference",
				StringUtils.pathEquals("/dummy1/bin/../dummy2/dummy3",
						"/dummy1/dummy2/dummy4"));
	}

	public void testConcatenateStringArrays() {
		String[] input1 = new String[] {"myString2"};
		String[] input2 = new String[] {"myString1", "myString2"};
		String[] result = StringUtils.concatenateStringArrays(input1, input2);
		org.junit.Assert.assertEquals(3, result.length);
		org.junit.Assert.assertEquals("myString2", result[0]);
		org.junit.Assert.assertEquals("myString1", result[1]);
		org.junit.Assert.assertEquals("myString2", result[2]);

		assertEquals(input1, StringUtils.concatenateStringArrays(input1, null));
		assertEquals(input2, StringUtils.concatenateStringArrays(null, input2));
		org.junit.Assert.assertNull(StringUtils.concatenateStringArrays(null, null));
	}

	public void testMergeStringArrays() {
		String[] input1 = new String[] {"myString2"};
		String[] input2 = new String[] {"myString1", "myString2"};
		String[] result = StringUtils.mergeStringArrays(input1, input2);
		org.junit.Assert.assertEquals(2, result.length);
		org.junit.Assert.assertEquals("myString2", result[0]);
		org.junit.Assert.assertEquals("myString1", result[1]);

		assertEquals(input1, StringUtils.mergeStringArrays(input1, null));
		assertEquals(input2, StringUtils.mergeStringArrays(null, input2));
		org.junit.Assert.assertNull(StringUtils.mergeStringArrays(null, null));
	}

	public void testSortStringArray() {
		String[] input = new String[] {"myString2"};
		input = StringUtils.addStringToArray(input, "myString1");
		org.junit.Assert.assertEquals("myString2", input[0]);
		org.junit.Assert.assertEquals("myString1", input[1]);

		StringUtils.sortStringArray(input);
		org.junit.Assert.assertEquals("myString1", input[0]);
		org.junit.Assert.assertEquals("myString2", input[1]);
	}

	public void testRemoveDuplicateStrings() {
		String[] input = new String[] {"myString2", "myString1", "myString2"};
		input = StringUtils.removeDuplicateStrings(input);
		org.junit.Assert.assertEquals("myString1", input[0]);
		org.junit.Assert.assertEquals("myString2", input[1]);
	}

	public void testSplitArrayElementsIntoProperties() {
		String[] input = new String[] {"key1=value1 ", "key2 =\"value2\""};
		Properties result = StringUtils.splitArrayElementsIntoProperties(input, "=");
		org.junit.Assert.assertEquals("value1", result.getProperty("key1"));
		org.junit.Assert.assertEquals("\"value2\"", result.getProperty("key2"));
	}

	public void testSplitArrayElementsIntoPropertiesAndDeletedChars() {
		String[] input = new String[] {"key1=value1 ", "key2 =\"value2\""};
		Properties result = StringUtils.splitArrayElementsIntoProperties(input, "=", "\"");
		org.junit.Assert.assertEquals("value1", result.getProperty("key1"));
		org.junit.Assert.assertEquals("value2", result.getProperty("key2"));
	}

	public void testTokenizeToStringArray() {
		String[] sa = StringUtils.tokenizeToStringArray("a,b , ,c", ",");
		org.junit.Assert.assertEquals(3, sa.length);
		org.junit.Assert.assertTrue("components are correct",
				sa[0].equals("a") && sa[1].equals("b") && sa[2].equals("c"));
	}

	public void testTokenizeToStringArrayWithNotIgnoreEmptyTokens() {
		String[] sa = StringUtils.tokenizeToStringArray("a,b , ,c", ",", true, false);
		org.junit.Assert.assertEquals(4, sa.length);
		org.junit.Assert.assertTrue("components are correct",
				sa[0].equals("a") && sa[1].equals("b") && sa[2].equals("") && sa[3].equals("c"));
	}

	public void testTokenizeToStringArrayWithNotTrimTokens() {
		String[] sa = StringUtils.tokenizeToStringArray("a,b ,c", ",", false, true);
		org.junit.Assert.assertEquals(3, sa.length);
		org.junit.Assert.assertTrue("components are correct",
				sa[0].equals("a") && sa[1].equals("b ") && sa[2].equals("c"));
	}

	public void testCommaDelimitedListToStringArrayWithNullProducesEmptyArray() {
		String[] sa = StringUtils.commaDelimitedListToStringArray(null);
		org.junit.Assert.assertTrue("String array isn't null with null input", sa != null);
		org.junit.Assert.assertTrue("String array length == 0 with null input", sa.length == 0);
	}

	public void testCommaDelimitedListToStringArrayWithEmptyStringProducesEmptyArray() {
		String[] sa = StringUtils.commaDelimitedListToStringArray("");
		org.junit.Assert.assertTrue("String array isn't null with null input", sa != null);
		org.junit.Assert.assertTrue("String array length == 0 with null input", sa.length == 0);
	}

	private void testStringArrayReverseTransformationMatches(String[] sa) {
		String[] reverse =
				StringUtils.commaDelimitedListToStringArray(StringUtils.arrayToCommaDelimitedString(sa));
		org.junit.Assert.assertEquals("Reverse transformation is equal",
				Arrays.asList(sa),
				Arrays.asList(reverse));
	}

	public void testDelimitedListToStringArrayWithComma() {
		String[] sa = StringUtils.delimitedListToStringArray("a,b", ",");
		org.junit.Assert.assertEquals(2, sa.length);
		org.junit.Assert.assertEquals("a", sa[0]);
		org.junit.Assert.assertEquals("b", sa[1]);
	}

	public void testDelimitedListToStringArrayWithSemicolon() {
		String[] sa = StringUtils.delimitedListToStringArray("a;b", ";");
		org.junit.Assert.assertEquals(2, sa.length);
		org.junit.Assert.assertEquals("a", sa[0]);
		org.junit.Assert.assertEquals("b", sa[1]);
	}

	public void testDelimitedListToStringArrayWithEmptyString() {
		String[] sa = StringUtils.delimitedListToStringArray("a,b", "");
		org.junit.Assert.assertEquals(3, sa.length);
		org.junit.Assert.assertEquals("a", sa[0]);
		org.junit.Assert.assertEquals(",", sa[1]);
		org.junit.Assert.assertEquals("b", sa[2]);
	}

	public void testDelimitedListToStringArrayWithNullDelimiter() {
		String[] sa = StringUtils.delimitedListToStringArray("a,b", null);
		org.junit.Assert.assertEquals(1, sa.length);
		org.junit.Assert.assertEquals("a,b", sa[0]);
	}

	public void testCommaDelimitedListToStringArrayMatchWords() {
		// Could read these from files
		String[] sa = new String[] {"foo", "bar", "big"};
		doTestCommaDelimitedListToStringArrayLegalMatch(sa);
		testStringArrayReverseTransformationMatches(sa);

		sa = new String[] {"a", "b", "c"};
		doTestCommaDelimitedListToStringArrayLegalMatch(sa);
		testStringArrayReverseTransformationMatches(sa);

		// Test same words
		sa = new String[] {"AA", "AA", "AA", "AA", "AA"};
		doTestCommaDelimitedListToStringArrayLegalMatch(sa);
		testStringArrayReverseTransformationMatches(sa);
	}

	public void testCommaDelimitedListToStringArraySingleString() {
		// Could read these from files
		String s = "woeirqupoiewuropqiewuorpqiwueopriquwopeiurqopwieur";
		String[] sa = StringUtils.commaDelimitedListToStringArray(s);
		org.junit.Assert.assertTrue("Found one String with no delimiters", sa.length == 1);
		org.junit.Assert.assertTrue("Single array entry matches input String with no delimiters",
				sa[0].equals(s));
	}

	public void testCommaDelimitedListToStringArrayWithOtherPunctuation() {
		// Could read these from files
		String[] sa = new String[] {"xcvwert4456346&*.", "///", ".!", ".", ";"};
		doTestCommaDelimitedListToStringArrayLegalMatch(sa);
	}

	/**
	 * We expect to see the empty Strings in the output.
	 */
	public void testCommaDelimitedListToStringArrayEmptyStrings() {
		// Could read these from files
		String[] sa = StringUtils.commaDelimitedListToStringArray("a,,b");
		org.junit.Assert.assertEquals("a,,b produces array length 3", 3, sa.length);
		org.junit.Assert.assertTrue("components are correct",
				sa[0].equals("a") && sa[1].equals("") && sa[2].equals("b"));

		sa = new String[] {"", "", "a", ""};
		doTestCommaDelimitedListToStringArrayLegalMatch(sa);
	}

	private void doTestCommaDelimitedListToStringArrayLegalMatch(String[] components) {
		StringBuffer sbuf = new StringBuffer();
		for (int i = 0; i < components.length; i++) {
			if (i != 0) {
				sbuf.append(",");
			}
			sbuf.append(components[i]);
		}
		String[] sa = StringUtils.commaDelimitedListToStringArray(sbuf.toString());
		org.junit.Assert.assertTrue("String array isn't null with legal match", sa != null);
		org.junit.Assert.assertEquals("String array length is correct with legal match", components.length, sa.length);
		org.junit.Assert.assertTrue("Output equals input", Arrays.equals(sa, components));
	}

	public void testEndsWithIgnoreCase() {
		String suffix = "fOo";
		org.junit.Assert.assertTrue(StringUtils.endsWithIgnoreCase("foo", suffix));
		org.junit.Assert.assertTrue(StringUtils.endsWithIgnoreCase("Foo", suffix));
		org.junit.Assert.assertTrue(StringUtils.endsWithIgnoreCase("barfoo", suffix));
		org.junit.Assert.assertTrue(StringUtils.endsWithIgnoreCase("barbarfoo", suffix));
		org.junit.Assert.assertTrue(StringUtils.endsWithIgnoreCase("barFoo", suffix));
		org.junit.Assert.assertTrue(StringUtils.endsWithIgnoreCase("barBarFoo", suffix));
		org.junit.Assert.assertTrue(StringUtils.endsWithIgnoreCase("barfoO", suffix));
		org.junit.Assert.assertTrue(StringUtils.endsWithIgnoreCase("barFOO", suffix));
		org.junit.Assert.assertTrue(StringUtils.endsWithIgnoreCase("barfOo", suffix));
		org.junit.Assert.assertFalse(StringUtils.endsWithIgnoreCase(null, suffix));
		org.junit.Assert.assertFalse(StringUtils.endsWithIgnoreCase("barfOo", null));
		org.junit.Assert.assertFalse(StringUtils.endsWithIgnoreCase("b", suffix));
	}

	public void testParseLocaleStringSunnyDay() throws Exception {
		Locale expectedLocale = Locale.UK;
		Locale locale = StringUtils.parseLocaleString(expectedLocale.toString());
		org.junit.Assert.assertNotNull("When given a bona-fide Locale string, must not return null.", locale);
		org.junit.Assert.assertEquals(expectedLocale, locale);
	}

	public void testParseLocaleStringWithMalformedLocaleString() throws Exception {
		Locale locale = StringUtils.parseLocaleString("_banjo_on_my_knee");
		org.junit.Assert.assertNotNull("When given a malformed Locale string, must not return null.", locale);
	}

	public void testParseLocaleStringWithEmptyLocaleStringYieldsNullLocale() throws Exception {
		Locale locale = StringUtils.parseLocaleString("");
		org.junit.Assert.assertNull("When given an empty Locale string, must return null.", locale);
	}

	/**
	 * <a href="http://opensource.atlassian.com/projects/spring/browse/SPR-8637">See SPR-8637</a>.
	 */
	public void testParseLocaleWithMultiSpecialCharactersInVariant() throws Exception {
		final String variant = "proper-northern";
		final String localeString = "en_GB_" + variant;
		Locale locale = StringUtils.parseLocaleString(localeString);
		org.junit.Assert.assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale
				.getVariant());
	}

	/**
	 * <a href="http://opensource.atlassian.com/projects/spring/browse/SPR-3671">See SPR-3671</a>.
	 */
	public void testParseLocaleWithMultiValuedVariant() throws Exception {
		final String variant = "proper_northern";
		final String localeString = "en_GB_" + variant;
		Locale locale = StringUtils.parseLocaleString(localeString);
		org.junit.Assert.assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
	}

	/**
	 * <a href="http://opensource.atlassian.com/projects/spring/browse/SPR-3671">See SPR-3671</a>.
	 */
	public void testParseLocaleWithMultiValuedVariantUsingSpacesAsSeparators() throws Exception {
		final String variant = "proper northern";
		final String localeString = "en GB " + variant;
		Locale locale = StringUtils.parseLocaleString(localeString);
		org.junit.Assert.assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
	}

	/**
	 * <a href="http://opensource.atlassian.com/projects/spring/browse/SPR-3671">See SPR-3671</a>.
	 */
	public void testParseLocaleWithMultiValuedVariantUsingMixtureOfUnderscoresAndSpacesAsSeparators() throws Exception {
		final String variant = "proper northern";
		final String localeString = "en_GB_" + variant;
		Locale locale = StringUtils.parseLocaleString(localeString);
		org.junit.Assert.assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
	}

	/**
	 * <a href="http://opensource.atlassian.com/projects/spring/browse/SPR-3671">See SPR-3671</a>.
	 */
	public void testParseLocaleWithMultiValuedVariantUsingSpacesAsSeparatorsWithLotsOfLeadingWhitespace() throws Exception {
		final String variant = "proper northern";
		final String localeString = "en GB            " + variant; // lots of whitespace
		Locale locale = StringUtils.parseLocaleString(localeString);
		org.junit.Assert.assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
	}

	/**
	 * <a href="http://opensource.atlassian.com/projects/spring/browse/SPR-3671">See SPR-3671</a>.
	 */
	public void testParseLocaleWithMultiValuedVariantUsingUnderscoresAsSeparatorsWithLotsOfLeadingWhitespace() throws Exception {
		final String variant = "proper_northern";
		final String localeString = "en_GB_____" + variant; // lots of underscores
		Locale locale = StringUtils.parseLocaleString(localeString);
		org.junit.Assert.assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
	}

	/**
	 * <a href="http://opensource.atlassian.com/projects/spring/browse/SPR-7779">See SPR-7779</a>.
	 */
	public void testParseLocaleWithInvalidCharacters() {
		try {
			StringUtils.parseLocaleString("%0D%0AContent-length:30%0D%0A%0D%0A%3Cscript%3Ealert%28123%29%3C/script%3E");
			org.junit.Assert.fail("Should have thrown IllegalArgumentException");
		}
		catch (IllegalArgumentException ex) {
			// expected
		}
	}

	/**
	 * See SPR-9420.
	 */
	public void testParseLocaleWithSameLowercaseTokenForLanguageAndCountry() {
		assertEquals("tr_TR", StringUtils.parseLocaleString("tr_tr").toString());
		assertEquals("bg_BG_vnt", StringUtils.parseLocaleString("bg_bg_vnt").toString());
	}
}
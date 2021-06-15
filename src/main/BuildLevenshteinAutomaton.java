/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.UTF32ToUTF8;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

//import org.apache.lucene.util.BytesRef;
//import org.apache.lucene.util.IntsRef;
//import org.apache.lucene.util.IntsRefBuilder;

// pushd /l/trunk/lucene/core; ../../gradlew jar; popd; javac -cp /l/trunk/lucene/core/build/libs/lucene-core-9.0.0-SNAPSHOT.jar BuildLevenshteinAutomaton.java; java -cp .:/l/trunk/lucene/core/build/libs/lucene-core-9.0.0-SNAPSHOT.jar BuildLevenshteinAutomaton lucene 1 true 2 false 128 > /x/tmp/out.dot; dot -Tpng /x/tmp/out.dot > /x/tmp/out.png

// javac -cp /l/buildfst/lucene/build/core/lucene-core-4.4-SNAPSHOT.jar BuildFST.java 

public class BuildLevenshteinAutomaton {

  // TODO: upgrade to https://github.com/rmuir/booleanparser ;)
  private static boolean parseBoolean(String value, String paramName) {
    if (value.equals("true")) {
      return true;
    } else if (value.equals("false")) {
      return false;
    } else {
      throw new RuntimeException(paramName + " must be \"true\" or \"false\"; got: \"" + value + "\"");
    }
  }

  // Copied from Lucene's FuzzyAutomatonBuilder!
  private static int[] stringToUTF32(String text) {
    int[] termText = new int[text.codePointCount(0, text.length())];
    for (int cp, i = 0, j = 0; i < text.length(); i += Character.charCount(cp)) {
      termText[j++] = cp = text.codePointAt(i);
    }
    return termText;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void main(String[] args) throws IOException {

    if (args.length != 5 && args.length != 6) {
      throw new RuntimeException("Usage: java BuildLevenshteinAutomaton <term> <edit-distance:1|2> <transposition-is-one-edit:true|false> <prefix-length:non-negative-int> <convert-to-utf8:true|false> [<alphabet-max>]");
    }

    String term = args[0];
    int[] codePoints = stringToUTF32(term);
    int termLength = codePoints.length;

    int editDistance;
    try {
      editDistance = Integer.parseInt(args[1]);
    } catch (NumberFormatException nfe) {
      throw new RuntimeException("could not parse edit-distance \"" + args[1] + "\" as integer", nfe);
    }

    if (editDistance != 1 && editDistance != 2) {
      throw new RuntimeException("edit-distance must be 1 or 2; got: " + editDistance);
    }

    boolean transpositionIsOneEdit = parseBoolean(args[2], "transposition-is-one-edit");
    
    int prefixLength;
    try {
      prefixLength = Integer.parseInt(args[3]);
    } catch (NumberFormatException nfe) {
      throw new RuntimeException("could not parse prefix-length \"" + args[1] + "\" as integer", nfe);
    }

    if (prefixLength < 0 || prefixLength > termLength) {
      throw new RuntimeException("prefix-length must be >= 0 and <= term's UTF32 length; got: " + prefixLength);
    }

    String prefix = UnicodeUtil.newString(codePoints, 0, prefixLength);

    boolean convertToUTF8 = parseBoolean(args[4], "convert-to-utf8");

    int alphaMax;

    if (args.length == 6) {
      try {
        alphaMax = Integer.parseInt(args[5]);
      } catch (NumberFormatException nfe) {
        throw new RuntimeException("could not parse alphabet-max \"" + args[5] + "\" as integer", nfe);
      }
    } else {
      // Accept all Unicode characters
      alphaMax = Character.MAX_CODE_POINT;
    }

    int[] suffix = new int[codePoints.length - prefixLength];
    System.arraycopy(codePoints, prefixLength, suffix, 0, suffix.length);

    Automaton a = new LevenshteinAutomata(suffix, alphaMax, transpositionIsOneEdit).toAutomaton(editDistance, prefix);
    if (convertToUTF8) {
      a = new UTF32ToUTF8().convert(a);
      a = Operations.determinize(a, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    System.out.println(a.toDot());
  }
}

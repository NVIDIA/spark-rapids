/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids

import java.sql.SQLException

import scala.collection.mutable.ListBuffer

/**
 * Regular expression parser based on a Pratt Parser design.
 *
 * The goal of this parser is to build a minimal AST that allows us
 * to validate that we can support the expression on the GPU. The goal
 * is not to parse with the level of detail that would be required if
 * we were building an evaluation engine. For example, operator precedence is
 * largely ignored but could be added if we need it later.
 *
 * The Java and cuDF regular expression documentation has been used as a reference:
 *
 * Java regex: https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
 * cuDF regex: https://docs.rapids.ai/api/libcudf/stable/md_regex.html
 *
 * The following blog posts provide some background on Pratt Parsers and parsing regex.
 *
 * - https://journal.stuffwithstuff.com/2011/03/19/pratt-parsers-expression-parsing-made-easy/
 * - https://matt.might.net/articles/parsing-regex-with-recursive-descent/
 */
class RegexParser(pattern: String) {

  /** index of current position within the string being parsed */
  private var pos = 0

  def parse(): RegexAST = {
    val ast = parseUntil(() => eof())
    if (!eof()) {
      throw new RegexUnsupportedException("failed to parse full regex")
    }
    ast
  }

  private def parseUntil(until: () => Boolean): RegexAST = {
    val term = parseTerm(() => until() || peek().contains('|'))
    if (!eof() && peek().contains('|')) {
      consumeExpected('|')
      RegexChoice(term, parseUntil(until))
    } else {
      term
    }
  }

  private def parseTerm(until: () => Boolean): RegexAST = {
    val sequence = RegexSequence(new ListBuffer())
    while (!eof() && !until()) {
      parseFactor(until) match {
        case RegexSequence(parts) =>
          sequence.parts ++= parts
        case other =>
          sequence.parts += other
      }
    }
    sequence
  }

  private def isValidQuantifierAhead(): Boolean = {
    if (peek().contains('{')) {
      val bookmark = pos
      consumeExpected('{')
      val q = parseQuantifierOrLiteralBrace()
      pos = bookmark
      q match {
        case _: QuantifierFixedLength | _: QuantifierVariableLength => true
        case _ => false
      }
    } else {
      false
    }
  }

  private def parseFactor(until: () => Boolean): RegexAST = {
    var base = parseBase()
    while (!eof() && !until()
        && (peek().exists(ch => ch == '*' || ch == '+' || ch == '?')
        || isValidQuantifierAhead())) {

      val quantifier = if (peek().contains('{')) {
        consumeExpected('{')
        parseQuantifierOrLiteralBrace().asInstanceOf[RegexQuantifier]
      } else {
        SimpleQuantifier(consume())
      }
      base = RegexRepetition(base, quantifier)
    }
    base
  }

  private def parseBase(): RegexAST = {
    consume() match {
      case '(' =>
        parseGroup()
      case '[' =>
        parseCharacterClass()
      case '\\' =>
        parseEscapedCharacter()
      case '\u0000' =>
        throw new RegexUnsupportedException(
          "cuDF does not support null characters in regular expressions", Some(pos))
      case '*' | '+' | '?' =>
        throw new RegexUnsupportedException(
          "base expression cannot start with quantifier", Some(pos))
      case other =>
        RegexChar(other)
    }
  }

  private def parseGroup(): RegexAST = {
    val captureGroup = if (pos + 1 < pattern.length
        && pattern.charAt(pos) == '?'
        && pattern.charAt(pos+1) == ':') {
      pos += 2
      false
    } else {
      true
    }
    val term = parseUntil(() => peek().contains(')'))
    consumeExpected(')')
    RegexGroup(captureGroup, term)
  }

  private def parseCharacterClass(): RegexCharacterClass = {
    val start = pos
    val characterClass = RegexCharacterClass(negated = false, characters = ListBuffer())
    // loop until the end of the character class or EOF
    var characterClassComplete = false
    while (!eof() && !characterClassComplete) {
      val ch = consume()
      ch match {
        case '[' =>
          // treat as a literal character and add to the character class
          characterClass.append(ch)
        case ']' if (!characterClass.negated && pos > start + 1) ||
            (characterClass.negated && pos > start + 2) =>
          // "[]" is not a valid character class
          // "[]a]" is a valid character class containing the characters "]" and "a"
          // "[^]a]" is a valid negated character class containing the characters "]" and "a"
          characterClassComplete = true
        case '^' if pos == start + 1 =>
          // Negates the character class, causing it to match a single character not listed in
          // the character class. Only valid immediately after the opening '['
          characterClass.negated = true
        case '\n' | '\r' | '\t' | '\b' | '\f' | '\u0007' =>
          // treat as a literal character and add to the character class
          characterClass.append(ch)
        case '\\' =>
          peek() match {
            case None =>
              throw new RegexUnsupportedException(
                s"Unclosed character class", Some(pos))
            case Some(ch) =>
              // typically an escaped metacharacter ('\\', '^', '-', ']', '+')
              // within the character class, but could be any escaped character
              characterClass.appendEscaped(consumeExpected(ch))
          }
        case '\u0000' =>
          throw new RegexUnsupportedException(
            "cuDF does not support null characters in regular expressions", Some(pos))
        case _ =>
          // check for range
          val start = ch
          peek() match {
            case Some('-') =>
              consumeExpected('-')
              peek() match {
                case Some(']') =>
                  // '-' at end of class e.g. "[abc-]"
                  characterClass.append(ch)
                  characterClass.append('-')
                case Some(end) =>
                  skip()
                  characterClass.appendRange(start, end)
                case _ =>
                  throw new RegexUnsupportedException(
                    "unexpected EOF while parsing character range",
                    Some(pos))
              }
            case _ =>
              // treat as supported literal character
              characterClass.append(ch)
          }
      }
    }
    if (!characterClassComplete) {
      throw new RegexUnsupportedException(
        s"Unclosed character class", Some(pos))
    }
    characterClass
  }


  /**
   * Parse a quantifier in one of the following formats:
   *
   * {n}
   * {n,}
   * {n,m} (only valid if m >= n)
   */
  private def parseQuantifierOrLiteralBrace(): RegexAST = {

    // assumes that '{' has already been consumed
    val start = pos

    def treatAsLiteralBrace() = {
      // this was not a quantifier, just a literal '{'
      pos = start + 1
      RegexChar('{')
    }

    consumeInt match {
      case Some(minLength) =>
        peek() match {
          case Some(',') =>
            consumeExpected(',')
            val max = consumeInt()
            if (peek().contains('}')) {
              consumeExpected('}')
              max match {
                case None =>
                  QuantifierVariableLength(minLength, None)
                case Some(m) =>
                  if (m >= minLength) {
                    QuantifierVariableLength(minLength, max)
                  } else {
                    treatAsLiteralBrace()
                  }
              }
            } else {
              treatAsLiteralBrace()
            }
          case Some('}') =>
            consumeExpected('}')
            QuantifierFixedLength(minLength)
          case _ =>
            treatAsLiteralBrace()
        }
      case None =>
        treatAsLiteralBrace()
    }
  }

  private def parseEscapedCharacter(): RegexAST = {
    peek() match {
      case None =>
        throw new RegexUnsupportedException("escape at end of string", Some(pos))
      case Some(ch) =>
        ch match {
          case 'A' | 'Z' | 'z' =>
            // string anchors
            consumeExpected(ch)
            RegexEscaped(ch)
          case 's' | 'S' | 'd' | 'D' | 'w' | 'W' =>
            // meta sequences
            consumeExpected(ch)
            RegexEscaped(ch)
          case 'B' | 'b' =>
            // word boundaries
            consumeExpected(ch)
            RegexEscaped(ch)
          case '[' | '\\' | '^' | '$' | '.' | '⎮' | '?' | '*' | '+' | '(' | ')' | '{' | '}' =>
            // escaped metacharacter
            consumeExpected(ch)
            RegexEscaped(ch)
          case 'x' =>
            consumeExpected(ch)
            parseHexDigit
          case _ if Character.isDigit(ch) =>
            parseOctalDigit
          case other =>
            throw new RegexUnsupportedException(
              s"invalid or unsupported escape character '$other'", Some(pos - 1))
        }
    }
  }

  private def isHexDigit(ch: Char): Boolean = ch.isDigit ||
    (ch >= 'a' && ch <= 'f') ||
    (ch >= 'A' && ch <= 'F')

  private def parseHexDigit: RegexHexDigit = {
    // \xhh      The character with hexadecimal value 0xhh
    // \x{h...h} The character with hexadecimal value 0xh...h
    //           (Character.MIN_CODE_POINT  <= 0xh...h <=  Character.MAX_CODE_POINT)

    val start = pos
    while (!eof() && isHexDigit(pattern.charAt(pos))) {
      pos += 1
    }
    val hexDigit = pattern.substring(start, pos)

    if (hexDigit.length < 2) {
      throw new RegexUnsupportedException(s"Invalid hex digit: $hexDigit")
    }

    val value = Integer.parseInt(hexDigit, 16)
    if (value < Character.MIN_CODE_POINT || value > Character.MAX_CODE_POINT) {
      throw new RegexUnsupportedException(s"Invalid hex digit: $hexDigit")
    }

    RegexHexDigit(hexDigit)
  }

  private def isOctalDigit(ch: Char): Boolean = ch >= '0' && ch <= '7'

  private def parseOctalDigit: RegexOctalChar = {
    // \0n   The character with octal value 0n (0 <= n <= 7)
    // \0nn  The character with octal value 0nn (0 <= n <= 7)
    // \0mnn The character with octal value 0mnn (0 <= m <= 3, 0 <= n <= 7)

    def parseOctalDigits(n: Integer): RegexOctalChar = {
      val octal = pattern.substring(pos, pos + n)
      pos += n
      RegexOctalChar(octal)
    }

    if (!eof() && isOctalDigit(pattern.charAt(pos))) {
      if (pos + 1 < pattern.length && isOctalDigit(pattern.charAt(pos + 1))) {
        if (pos + 2 < pattern.length && isOctalDigit(pattern.charAt(pos + 2))
            && pattern.charAt(pos) <= '3') {
          parseOctalDigits(3)
        } else {
          parseOctalDigits(2)
        }
      } else {
        parseOctalDigits(1)
      }
    } else {
      throw new RegexUnsupportedException(
        "Invalid octal digit", Some(pos))
    }
  }

  /** Determine if we are at the end of the input */
  private def eof(): Boolean = pos == pattern.length

  /** Advance the index by one */
  private def skip(): Unit = {
    if (eof()) {
      throw new RegexUnsupportedException("Unexpected EOF", Some(pos))
    }
    pos += 1
  }

  /** Get the next character and advance the index by one */
  private def consume(): Char = {
    if (eof()) {
      throw new RegexUnsupportedException("Unexpected EOF", Some(pos))
    } else {
      pos += 1
      pattern.charAt(pos - 1)
    }
  }

  /** Consume the next character if it is the one we expect */
  private def consumeExpected(expected: Char): Char = {
    val consumed = consume()
    if (consumed != expected) {
      throw new RegexUnsupportedException(
        s"Expected '$expected' but found '$consumed'", Some(pos-1))
    }
    consumed
  }

  /** Peek at the next character without consuming it */
  private def peek(): Option[Char] = {
    if (eof()) {
      None
    } else {
      Some(pattern.charAt(pos))
    }
  }

  private def consumeInt(): Option[Int] = {
    val start = pos
    while (!eof() && peek().exists(_.isDigit)) {
      skip()
    }
    if (start == pos) {
      None
    } else {
      Some(pattern.substring(start, pos).toInt)
    }
  }

}

/**
 * Transpile Java/Spark regular expression to a format that cuDF supports, or throw an exception
 * if this is not possible.
 *
 * @param replace True if performing a replacement (regexp_replace), false
 *                if matching only (rlike)
 */
class CudfRegexTranspiler(replace: Boolean) {

  // cuDF throws a "nothing to repeat" exception for many of the edge cases that are
  // rejected by the transpiler
  private val nothingToRepeat = "nothing to repeat"

  /**
   * Parse Java regular expression and translate into cuDF regular expression.
   *
   * @param pattern Regular expression that is valid in Java's engine
   * @return Regular expression in cuDF format
   */
  def transpile(pattern: String): String = {
    // parse the source regular expression
    val regex = new RegexParser(pattern).parse()
    // validate that the regex is supported by cuDF
    val cudfRegex = rewrite(regex)
    // write out to regex string, performing minor transformations
    // such as adding additional escaping
    cudfRegex.toRegexString
  }

  private def rewrite(regex: RegexAST): RegexAST = {
    regex match {

      case RegexChar(ch) => ch match {
        case '.' =>
          // workaround for https://github.com/rapidsai/cudf/issues/9619
          RegexCharacterClass(negated = true, ListBuffer(RegexChar('\r'), RegexChar('\n')))
        case '$' =>
          // see https://github.com/NVIDIA/spark-rapids/issues/4533
          throw new RegexUnsupportedException("line anchor $ is not supported")
        case _ =>
          regex
      }

      case RegexOctalChar(_) =>
        // see https://github.com/NVIDIA/spark-rapids/issues/4288
        throw new RegexUnsupportedException(
          s"cuDF does not support octal digits consistently with Spark")

      case RegexHexDigit(_) =>
        // see https://github.com/NVIDIA/spark-rapids/issues/4486
        throw new RegexUnsupportedException(
          s"cuDF does not support hex digits consistently with Spark")

      case RegexEscaped(ch) => ch match {
        case 'D' =>
          // see https://github.com/NVIDIA/spark-rapids/issues/4475
          throw new RegexUnsupportedException("non-digit class \\D is not supported")
        case 'W' =>
          // see https://github.com/NVIDIA/spark-rapids/issues/4475
          throw new RegexUnsupportedException("non-word class \\W is not supported")
        case 'b' | 'B' =>
          // see https://github.com/NVIDIA/spark-rapids/issues/4517
          throw new RegexUnsupportedException("word boundaries are not supported")
        case 's' | 'S' =>
          // see https://github.com/NVIDIA/spark-rapids/issues/4528
          throw new RegexUnsupportedException("whitespace classes are not supported")
        case 'z' =>
          if (replace) {
            // see https://github.com/NVIDIA/spark-rapids/issues/4425
            throw new RegexUnsupportedException(
              "string anchor \\z is not supported in replace mode")
          }
          // cuDF does not support "\z" but supports "$", which is equivalent
          RegexChar('$')
        case 'Z' =>
          // see https://github.com/NVIDIA/spark-rapids/issues/4532
          throw new RegexUnsupportedException("string anchor \\Z is not supported")
        case _ =>
          regex
      }

      case RegexCharacterRange(_, _) =>
        regex

      case RegexCharacterClass(negated, characters) =>
        characters.foreach {
          case RegexChar(ch) if ch == '[' || ch == ']' =>
            // examples:
            // - "[a[]" should match the literal characters "a" and "["
            // - "[a-b[c-d]]" is supported by Java but not cuDF
            throw new RegexUnsupportedException("nested character classes are not supported")
          case _ =>
        }
        val components: Seq[RegexCharacterClassComponent] = characters
          .map(x => rewrite(x).asInstanceOf[RegexCharacterClassComponent])

        if (negated) {
          // There are differences between cuDF and Java handling of newlines
          // for negative character matches. The expression `[^a]` will match
          // `\r` and `\n` in Java but not in cuDF, so we replace `[^a]` with
          // `(?:[\r\n]|[^a])`. We also have to take into account whether any
          // newline characters are included in the character range.
          //
          // Examples:
          //
          // `[^a]`     => `(?:[\r\n]|[^a])`
          // `[^a\r]`   => `(?:[\n]|[^a])`
          // `[^a\n]`   => `(?:[\r]|[^a])`
          // `[^a\r\n]` => `[^a]`
          // `[^\r\n]`  => `[^\r\n]`

          val linefeedCharsInPattern = components.flatMap {
            case RegexChar(ch) if ch == '\n' || ch == '\r' => Seq(ch)
            case RegexEscaped(ch) if ch == 'n' => Seq('\n')
            case RegexEscaped(ch) if ch == 'r' => Seq('\r')
            case _ => Seq.empty
          }

          val onlyLinefeedChars = components.length == linefeedCharsInPattern.length

          val negatedNewlines = Seq('\r', '\n').diff(linefeedCharsInPattern.distinct)

          if (onlyLinefeedChars && linefeedCharsInPattern.length == 2) {
            // special case for `[^\r\n]` and `[^\\r\\n]`
            RegexCharacterClass(negated = true, ListBuffer(components: _*))
          } else if (negatedNewlines.isEmpty) {
            RegexCharacterClass(negated = true, ListBuffer(components: _*))
          } else {
            RegexGroup(capture = false,
              RegexChoice(
                RegexCharacterClass(negated = false,
                  characters = ListBuffer(negatedNewlines.map(RegexChar): _*)),
                RegexCharacterClass(negated = true, ListBuffer(components: _*))))
          }
        } else {
          RegexCharacterClass(negated, ListBuffer(components: _*))
        }

      case RegexSequence(parts) =>
        if (parts.isEmpty) {
          // examples: "", "()", "a|", "|b"
          throw new RegexUnsupportedException("empty sequence not supported")
        }
        if (isRegexChar(parts.head, '|') || isRegexChar(parts.last, '|')) {
          // examples: "a|", "|b"
          throw new RegexUnsupportedException(nothingToRepeat)
        }
        if (isRegexChar(parts.head, '{')) {
          // example: "{"
          // cuDF would treat this as a quantifier even though in this
          // context (being at the start of a sequence) it is not quantifying anything
          // note that we could choose to escape this in the transpiler rather than
          // falling back to CPU
          throw new RegexUnsupportedException(nothingToRepeat)
        }
        if (parts.forall(isBeginOrEndLineAnchor)) {
          throw new RegexUnsupportedException(
            "sequences that only contain '^' or '$' are not supported")
        }
        RegexSequence(parts.map(rewrite))

      case RegexRepetition(base, quantifier) => (base, quantifier) match {
        case (_, SimpleQuantifier(ch)) if replace && "?*".contains(ch) =>
          // example: pattern " ?", input "] b[", replace with "X":
          // java: X]XXbX[X
          // cuDF: XXXX] b[
          // see https://github.com/NVIDIA/spark-rapids/issues/4468
          throw new RegexUnsupportedException(
            "regexp_replace on GPU does not support repetition with ? or *")

        case (RegexEscaped(ch), _) if ch != 'd' && ch != 'w' =>
          // example: "\B?"
          throw new RegexUnsupportedException(nothingToRepeat)

        case (RegexChar(a), _) if "$^".contains(a) =>
          // example: "$*"
          throw new RegexUnsupportedException(nothingToRepeat)

        case (RegexRepetition(_, _), _) =>
          // example: "a*+"
          throw new RegexUnsupportedException(nothingToRepeat)

        case _ =>
          RegexRepetition(rewrite(base), quantifier)

      }

      case RegexChoice(l, r) =>
        val ll = rewrite(l)
        val rr = rewrite(r)

        // cuDF does not support repetition on one side of a choice, such as "a*|a"
        def isRepetition(e: RegexAST): Boolean = {
          e match {
            case RegexRepetition(_, _) => true
            case RegexGroup(_, term) => isRepetition(term)
            case RegexSequence(parts) if parts.nonEmpty => isRepetition(parts.last)
            case _ => false
          }
        }
        if (isRepetition(ll) || isRepetition(rr)) {
          throw new RegexUnsupportedException(nothingToRepeat)
        }

        // cuDF does not support terms ending with line anchors on one side
        // of a choice, such as "^|$"
        def endsWithLineAnchor(e: RegexAST): Boolean = {
          e match {
            case RegexSequence(parts) if parts.nonEmpty =>
              isBeginOrEndLineAnchor(parts.last)
            case _ => false
          }
        }
        if (endsWithLineAnchor(ll) || endsWithLineAnchor(rr)) {
          throw new RegexUnsupportedException(nothingToRepeat)
        }

        RegexChoice(ll, rr)

      case RegexGroup(capture, term) =>
        RegexGroup(capture, rewrite(term))

      case other =>
        throw new RegexUnsupportedException(s"Unhandled expression in transpiler: $other")
    }
  }

  private def isBeginOrEndLineAnchor(regex: RegexAST): Boolean = regex match {
    case RegexSequence(parts) => parts.nonEmpty && parts.forall(isBeginOrEndLineAnchor)
    case RegexGroup(_, term) => isBeginOrEndLineAnchor(term)
    case RegexChoice(l, r) => isBeginOrEndLineAnchor(l) && isBeginOrEndLineAnchor(r)
    case RegexRepetition(term, _) => isBeginOrEndLineAnchor(term)
    case RegexChar(ch) => ch == '^' || ch == '$'
    case RegexEscaped('z') => true // \z gets translated to $
    case _ => false
  }

  private def isRegexChar(expr: RegexAST, value: Char): Boolean = expr match {
    case RegexChar(ch) => ch == value
    case _ => false
  }
}

sealed trait RegexAST {
  def children(): Seq[RegexAST]
  def toRegexString: String
}

sealed case class RegexSequence(parts: ListBuffer[RegexAST]) extends RegexAST {
  override def children(): Seq[RegexAST] = parts
  override def toRegexString: String = parts.map(_.toRegexString).mkString
}

sealed case class RegexGroup(capture: Boolean, term: RegexAST) extends RegexAST {
  override def children(): Seq[RegexAST] = Seq(term)
  override def toRegexString: String = if (capture) {
    s"(${term.toRegexString})"
  } else {
    s"(?:${term.toRegexString})"
  }
}

sealed case class RegexChoice(a: RegexAST, b: RegexAST) extends RegexAST {
  override def children(): Seq[RegexAST] = Seq(a, b)
  override def toRegexString: String = s"${a.toRegexString}|${b.toRegexString}"
}

sealed case class RegexRepetition(a: RegexAST, quantifier: RegexQuantifier) extends RegexAST {
  override def children(): Seq[RegexAST] = Seq(a)
  override def toRegexString: String = s"${a.toRegexString}${quantifier.toRegexString}"
}

sealed trait RegexQuantifier extends RegexAST

sealed case class SimpleQuantifier(ch: Char) extends RegexQuantifier {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = ch.toString
}

sealed case class QuantifierFixedLength(length: Int)
    extends RegexQuantifier {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = {
    s"{$length}"
  }
}

sealed case class QuantifierVariableLength(minLength: Int, maxLength: Option[Int])
    extends RegexQuantifier{
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = {
    maxLength match {
      case Some(max) =>
        s"{$minLength,$max}"
      case _ =>
        s"{$minLength,}"
    }
  }
}

sealed trait RegexCharacterClassComponent extends RegexAST

sealed case class RegexHexDigit(a: String) extends RegexCharacterClassComponent {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"\\x$a"
}

sealed case class RegexOctalChar(a: String) extends RegexCharacterClassComponent {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"\\$a"
}

sealed case class RegexChar(a: Char) extends RegexCharacterClassComponent {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"$a"
}

sealed case class RegexEscaped(a: Char) extends RegexCharacterClassComponent{
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"\\$a"
}

sealed case class RegexCharacterRange(start: Char, end: Char)
  extends RegexCharacterClassComponent{
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"$start-$end"
}

sealed case class RegexCharacterClass(
    var negated: Boolean,
    var characters: ListBuffer[RegexCharacterClassComponent])
  extends RegexAST {

  override def children(): Seq[RegexAST] = characters
  def append(ch: Char): Unit = {
    characters += RegexChar(ch)
  }

  def appendEscaped(ch: Char): Unit = {
    characters += RegexEscaped(ch)
  }

  def appendRange(start: Char, end: Char): Unit = {
    characters += RegexCharacterRange(start, end)
  }

  override def toRegexString: String = {
    val builder = new StringBuilder("[")
    if (negated) {
      builder.append("^")
    }
    for (a <- characters) {
      a match {
        case RegexChar(ch) if requiresEscaping(ch) =>
          // cuDF has stricter escaping requirements for certain characters
          // within a character class compared to Java or Python regex
          builder.append(s"\\$ch")
        case other =>
          builder.append(other.toRegexString)
      }
    }
    builder.append("]")
    builder.toString()
  }

  private def requiresEscaping(ch: Char): Boolean = {
    // there are likely other cases that we will need to add here but this
    // covers everything we have seen so far during fuzzing
    ch match {
      case '-' =>
        // cuDF requires '-' to be escaped when used as a character within a character
        // to disambiguate from the character range syntax 'a-b'
        true
      case _ =>
        false
    }
  }
}

class RegexUnsupportedException(message: String, index: Option[Int] = None)
  extends SQLException {
  override def getMessage: String = {
    index match {
      case Some(i) => s"$message near index $i"
      case _ => message
    }
  }
}
/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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
  private var i = 0

  def parse(): RegexAST = {
    val ast = parseInternal()
    if (!eof()) {
      throw new RegexUnsupportedException("failed to parse full regex")
    }
    ast
  }

  private def parseInternal(): RegexAST = {
    val term = parseTerm(() => peek().contains('|'))
    if (!eof() && peek().contains('|')) {
      consumeExpected('|')
      RegexChoice(term, parseInternal())
    } else {
      term
    }
  }

  private def parseTerm(until: () => Boolean): RegexAST = {
    val sequence = RegexSequence(new ListBuffer())
    while (!eof() && !until()) {
      parseFactor() match {
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
      val bookmark = i
      consumeExpected('{')
      val q = parseQuantifierOrLiteralBrace()
      i = bookmark
      q match {
        case _: QuantifierFixedLength | _: QuantifierVariableLength => true
        case _ => false
      }
    } else {
      false
    }
  }

  private def parseFactor(): RegexAST = {
    var base = parseBase()
    while (!eof() && (peek().exists(ch => ch == '*' || ch == '+' || ch == '?')
      || isValidQuantifierAhead())) {

      if (peek().contains('{')) {
        consumeExpected('{')
        base = RegexRepetition(base, parseQuantifierOrLiteralBrace().asInstanceOf[RegexQuantifier])
      } else {
        base = RegexRepetition(base, SimpleQuantifier(consume()))
      }
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
          "cuDF does not support null characters in regular expressions", Some(i))
      case other =>
        RegexChar(other)
    }
  }

  private def parseGroup(): RegexAST = {
    val term = parseTerm(() => peek().contains(')'))
    consumeExpected(')')
    RegexGroup(term)
  }

  private def parseCharacterClass(): RegexCharacterClass = {
    val start = i
    val characterClass = RegexCharacterClass(negated = false, characters = ListBuffer())
    // loop until the end of the character class or EOF
    var characterClassComplete = false
    while (!eof() && !characterClassComplete) {
      val ch = consume()
      ch match {
        case '[' =>
          // treat as a literal character and add to the character class
          characterClass.append(ch)
        case ']' =>
          characterClassComplete = true
        case '^' if i == start + 1 =>
          // Negates the character class, causing it to match a single character not listed in
          // the character class. Only valid immediately after the opening '['
          characterClass.negated = true
        case '\n' | '\r' | '\t' | '\b' | '\f' | '\007' =>
          // treat as a literal character and add to the character class
          characterClass.append(ch)
        case '\\' =>
          peek() match {
            case None =>
              throw new RegexUnsupportedException(
                s"unexpected EOF while parsing escaped character", Some(i))
            case Some(ch) =>
              ch match {
                case '\\' | '^' | '-' | ']' | '+' =>
                  // escaped metacharacter within character class
                  characterClass.appendEscaped(consumeExpected(ch))
              }
          }
        case '\u0000' =>
          throw new RegexUnsupportedException(
            "cuDF does not support null characters in regular expressions", Some(i))
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
              }
            case _ =>
              // treat as supported literal character
              characterClass.append(ch)
          }
      }
    }
    if (!characterClassComplete) {
      throw new RegexUnsupportedException(
        s"unexpected EOF while parsing character class", Some(i))
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
    val start = i

    def treatAsLiteralBrace() = {
      // this was not a quantifier, just a literal '{'
      i = start + 1
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
        throw new RegexUnsupportedException("escape at end of string", Some(i))
      case Some(ch) =>
        consumeExpected(ch)
        ch match {
          case 'A' | 'Z' =>
            // BOL / EOL anchors
            RegexEscaped(ch)
          case 's' | 'S' | 'd' | 'D' | 'w' | 'W' =>
            // meta sequences
            RegexEscaped(ch)
          case 'B' | 'b' =>
            // word boundaries
            RegexEscaped(ch)
          case '[' | '\\' | '^' | '$' | '.' | '⎮' | '?' | '*' | '+' | '(' | ')' | '{' | '}' =>
            // escaped metacharacter
            RegexEscaped(ch)
          case 'x' =>
            parseHexDigit
          case _ if ch.isDigit =>
            parseOctalDigit
          case other =>
            throw new RegexUnsupportedException(
              s"invalid or unsupported escape character '$other'", Some(i - 1))
        }
    }
  }

  private def parseHexDigit: RegexAST = {

    def isHexDigit(ch: Char): Boolean = ch.isDigit ||
      (ch >= 'a' && ch <= 'f') ||
      (ch >= 'A' && ch <= 'F')

    if (i + 1 < pattern.length
      && isHexDigit(pattern.charAt(i))
      && isHexDigit(pattern.charAt(i+1))) {
      val hex = pattern.substring(i, i + 2)
      i += 2
      RegexHexChar(hex)
    } else {
      throw new RegexUnsupportedException(
        "Invalid hex digit", Some(i))
    }
  }

  private def parseOctalDigit = {

    if (i + 2 < pattern.length
        && pattern.charAt(i).isDigit
        && pattern.charAt(i+1).isDigit
        && pattern.charAt(i+2).isDigit) {
        val hex = pattern.substring(i, i + 2)
      i += 3
      RegexOctalChar(hex)
    } else {
      throw new RegexUnsupportedException(
        "Invalid octal digit", Some(i))
    }
  }

  /** Determine if we are at the end of the input */
  private def eof(): Boolean = i == pattern.length

  /** Advance the index by one */
  private def skip(): Unit = {
    if (eof()) {
      throw new RegexUnsupportedException("Unexpected EOF", Some(i))
    }
    i += 1
  }

  /** Get the next character and advance the index by one */
  private def consume(): Char = {
    if (eof()) {
      throw new RegexUnsupportedException("Unexpected EOF", Some(i))
    } else {
      i += 1
      pattern.charAt(i - 1)
    }
  }

  /** Consume the next character if it is the one we expect */
  private def consumeExpected(expected: Char): Char = {
    val consumed = consume()
    if (consumed != expected) {
      throw new RegexUnsupportedException(
        s"Expected '$expected' but found '$consumed'", Some(i-1))
    }
    consumed
  }

  /** Peek at the next character without consuming it */
  private def peek(): Option[Char] = {
    if (eof()) {
      None
    } else {
      Some(pattern.charAt(i))
    }
  }

  private def consumeInt(): Option[Int] = {
    val start = i
    while (!eof() && peek().exists(_.isDigit)) {
      skip()
    }
    if (start == i) {
      None
    } else {
      Some(pattern.substring(start, i).toInt)
    }
  }

}

/**
 * Transpile Java/Spark regular expression to a format that cuDF supports, or throw an exception
 * if this is not possible.
 */
class CudfRegexTranspiler {

  val nothingToRepeat = "nothing to repeat"

  def transpile(pattern: String): String = {
    // parse the source regular expression
    val regex = new RegexParser(pattern).parse()
    // validate that the regex is supported by cuDF
    validate(regex)
    // write out to regex string, performing minor transformations
    // such as adding additional escaping
    regex.toRegexString
  }

  private def validate(regex: RegexAST): Unit = {
    regex match {
      case RegexGroup(RegexSequence(parts)) if parts.isEmpty =>
        // example: "()"
        throw new RegexUnsupportedException("cuDF does not support empty groups")
      case RegexRepetition(RegexEscaped(_), _) =>
        // example: "\B?"
        throw new RegexUnsupportedException(nothingToRepeat)
      case RegexRepetition(RegexChar(a), _) if "$^".contains(a) =>
        // example: "$*"
        throw new RegexUnsupportedException(nothingToRepeat)
      case RegexRepetition(RegexRepetition(_, _), _) =>
        // example: "a*+"
        throw new RegexUnsupportedException(nothingToRepeat)
      case RegexChoice(l, r) =>
        (l, r) match {
          // check for empty left-hand side caused by ^ or $ or a repetition
          case (RegexSequence(a), _) =>
            a.lastOption match {
              case None =>
                // example: "|a"
                throw new RegexUnsupportedException(nothingToRepeat)
              case Some(RegexChar(ch)) if ch == '$' || ch == '^' =>
                // example: "^|a"
                throw new RegexUnsupportedException(nothingToRepeat)
              case Some(RegexRepetition(_, _)) =>
                // example: "a*|a"
                throw new RegexUnsupportedException(nothingToRepeat)
              case _ =>
            }
          // check for empty right-hand side caused by ^ or $
          case (_, RegexSequence(b)) =>
            b.headOption match {
              case None =>
                // example: "|b"
                throw new RegexUnsupportedException(nothingToRepeat)
              case Some(RegexChar(ch)) if ch == '$' || ch == '^' =>
                // example: "a|$"
                throw new RegexUnsupportedException(nothingToRepeat)
              case _ =>
            }
          case (RegexRepetition(_, _), _) =>
            // example: "a*|a"
            throw new RegexUnsupportedException(nothingToRepeat)
          case _ =>
        }

      case RegexSequence(parts) =>
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
        parts.foreach {
          case RegexEscaped(ch) if ch == 'b' || ch == 'B' =>
            // example: "a\Bb"
            // this needs further analysis to determine why words boundaries behave
            // differently between Java and cuDF
            throw new RegexUnsupportedException("word boundaries are not supported")
          case _ =>
        }
      case RegexCharacterClass(_, characters) =>
        characters.foreach {
          case RegexChar(ch) if ch == '[' || ch == ']' =>
            // examples:
            // - "[a[]" should match the literal characters "a" and "["
            // - "[a-b[c-d]]" is supported by Java but not cuDF
            throw new RegexUnsupportedException("nested character classes are not supported")
          case _ =>
        }

      case _ =>
    }

    // walk down the tree and validate children
    regex.children().foreach(validate)
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

sealed case class RegexGroup(term: RegexAST) extends RegexAST {
  override def children(): Seq[RegexAST] = Seq(term)
  override def toRegexString: String = s"(${term.toRegexString})"
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

sealed case class RegexHexChar(a: String) extends RegexCharacterClassComponent {
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
      case Some(i) => s"$message at index $index"
      case _ => message
    }
  }
}
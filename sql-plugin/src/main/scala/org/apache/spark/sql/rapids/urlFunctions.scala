/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.{ColumnVector, DType, RegexProgram, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuParseUrl {
  private val HOST = "HOST"
  private val PATH = "PATH"
  private val QUERY = "QUERY"
  private val REF = "REF"
  private val PROTOCOL = "PROTOCOL"
  private val FILE = "FILE"
  private val AUTHORITY = "AUTHORITY"
  private val USERINFO = "USERINFO"
  private val REGEXPREFIX = """(&|^)("""
  private val REGEXSUBFIX = "=)([^&]*)"
  // scalastyle:off line.size.limit
  //                                        
  // private val HOST_REGEX   = """^(?:(?:([^:/?#]+):)?(?://((?:(?:(?:[^\@]*)@)?(\[[\w%.:]+\]|[^/#:?]*))(?::[0-9]+)?))?(([^?#]*)(\?[^#]*)?)(#[\w\-_.!~*'();/?:@&=+$,[\]%]*)?)$"""
  //                            """^(?:(?:(  [^:/?#]+):)?(?://(  (?:(?:(?:[^\@/]*)@)?(\[[\w%.:]+\]|[^/#:?]*))(?::[0-9]+)?))?(([^?#]*)(\?[^#]*)?)(#.*)?)$"""
  private val HOST_REGEX      = """^(?:(?:(?:[\w+\-.]+):)?(?:[0-9]+[^\\\s#]*|(?://(?:(?:(?:(?:[\w%\-_.!~*'();:&=+$,]*)@)?(\[[\w%.:]+\]|[^/\\\s#:?"<>\^`{|}]*))(?::[0-9]+)?))?(?:[^?#[\]\\\s"<>\^`{|}]*)?(?:\?[^\\\s"#<>\^`{|}]*)?)(?:#[\w\-_.!~*'();/?:@&=+$,[\]%]*)?)$"""
  private val PATH_REGEX      = """^(?:[\w+\-.]+:[^/#][^/#][^#]*|(?:[\w+\-.]+:)?(?://(?:[\w%\-_.!~*'();:&=+$,]*@)?(?:\[[\w%.:]+\]|[^/\\\s#:?"<>\^`{|}]*)(?::[0-9]+)?)?([^?#[\]\\\s"<>\^`{|}]*)?(?:\?[^\\\s"#<>\^`{|}]*)?)(?:#[\w\-_.!~*'();/?:@&=+$,[\]%]*)?$"""
  private val QUERY_REGEX     = """^(?:(?:(?:[\w+\-.]+):)?(?:[0-9]+[^\\\s#]*|(?://(?:(?:(?:(?:[\w%\-_.!~*'();:&=+$,]*)@)?(?:\[[\w%.:]+\]|[^/\\\s#:?"<>\^`{|}]*))(?::[0-9]+)?))?(?:[^?#[\]\\\s"<>\^`{|}]*)?(\?[^\\\s"#<>\^`{|}]*)?)(?:#[\w\-_.!~*'();/?:@&=+$,[\]%]*)?)$"""
  private val REF_REGEX       = """^(?:(?:(?:[\w+\-.]+):)?(?:[0-9]+[^\\\s#]*|(?://(?:(?:(?:(?:[\w%\-_.!~*'();:&=+$,]*)@)?(?:\[[\w%.:]+\]|[^/\\\s#:?"<>\^`{|}]*))(?::[0-9]+)?))?(?:[^?#[\]\\\s"<>\^`{|}]*)?(?:\?[^\\\s"#<>\^`{|}]*)?)(#[\w\-_.!~*'();/?:@&=+$,[\]%]*)?)$"""
  private val PROTOCOL_REGEX  = """^(?:(?:([\w+\-.]+):)?(?:(?:[0-9]+[^\\\s#]*|(?://(?:(?:(?:[\w%\-_.!~*'();:&=+$,]*)@)?(?:\[[\w%.:]+\]|[^/\\\s#:?"<>\^`{|}]*))(?::[0-9]+)?))?(?:[^?#[\]\\\s"<>\^`{|}]*)?(?:\?[^\\\s"#<>\^`{|}]*)?)(?:#[\w\-_.!~*'();/?:@&=+$,[\]%]*)?)$"""
  private val FILE_REGEX      = """^(?:[\w+\-.]+:[^/#][^/#][^#]*|(?:[\w+\-.]+:)?(?://(?:[\w%\-_.!~*'();:&=+$,]*@)?(?:\[[\w%.:]+\]|[^/\\\s#:?"<>\^`{|}]*)(?::[0-9]+)?)?((?:[^?#[\]\\\s"<>\^`{|}]*)?(?:\?[^\\\s"#<>\^`{|}]*)?))(?:#[\w\-_.!~*'();/?:@&=+$,[\]%]*)?$"""
  private val AUTHORITY_REGEX = """^(?:(?:(?:[\w+\-.]+):)?(?:[0-9]+[^\\\s#]*|(?://((?:(?:(?:[\w%\-_.!~*'();:&=+$,]*)@)?(?:\[[\w%.:]+\]|[^/\\\s#:?"<>\^`{|}]*))(?::[0-9]+)?))?(?:[^?#[\]\\\s"<>\^`{|}]*)?(?:\?[^\\\s"#<>\^`{|}]*)?)(?:#[\w\-_.!~*'();/?:@&=+$,[\]%]*)?)$"""
  private val USERINFO_REGEX  = """^(?:(?:(?:[\w+\-.]+):)?(?:[0-9]+[^\\\s#]*|(?://(?:(?:(?:([\w%\-_.!~*'();:&=+$,]*)@)?(?:\[[\w%.:]+\]|[^/\\\s#:?"<>\^`{|}]*))(?::[0-9]+)?))?(?:[^?#[\]\\\s"<>\^`{|}]*)?(?:\?[^\\\s"#<>\^`{|}]*)?)(?:#[\w\-_.!~*'();/?:@&=+$,[\]%]*)?)$"""
  // HostName parsing followed rules in java URI lib:
  // hostname      = domainlabel [ "." ] | 1*( domainlabel "." ) toplabel [ "." ]
  // domainlabel   = alphanum | alphanum *( alphanum | "-" ) alphanum
  // toplabel      = alpha | alpha *( alphanum | "-" ) alphanum
  val hostnameRegex = """((([0-9a-zA-Z]|[0-9a-zA-Z][0-9a-zA-Z\-]*[0-9a-zA-Z])|(([0-9a-zA-Z]|[0-9a-zA-Z][0-9a-zA-Z\-]*[0-9a-zA-Z])\.)+([a-zA-Z]|[a-zA-Z][\w\-]*[a-zA-Z]))\.?)"""
  val ipv4Regex = """(((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9]))"""
  val simpleIpv6Regex = """(\[[\w%.:]+])"""
  // based on https://stackoverflow.com/questions/53497/regular-expression-that-matches-valid-ipv6-addresses
  val ipv6Regex1 = """([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?"""         
  // 1:2:3:4:5:6:7:8
  val ipv6Regex2 = """([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?:"""                        
  // 1::                              1:2:3:4:5:6:7::
  val ipv6Regex3 = """(([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)"""        
  // 1::8             1:2:3:4:5:6::8  1:2:3:4:5:6::8
  val ipv6Regex4 = """(([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?)""" 
  // 1::7:8           1:2:3:4:5::7:8  1:2:3:4:5::8
  val ipv6Regex5 = """(([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?)""" 
  // 1::6:7:8         1:2:3:4::6:7:8  1:2:3:4::8
  val ipv6Regex6 = """(([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?)""" 
  // 1::5:6:7:8       1:2:3::5:6:7:8  1:2:3::8
  val ipv6Regex7 = """(([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?)""" 
  // 1::4:5:6:7:8     1:2::4:5:6:7:8  1:2::8
  val ipv6Regex8 = """([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:((:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?))"""      
  // 1::3:4:5:6:7:8   1::3:4:5:6:7:8  1::8
  val ipv6Regex9 = """(:((:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?(:[0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?|:))"""                    
  // ::2:3:4:5:6:7:8  ::2:3:4:5:6:7:8 ::8       ::
  val ipv6Regex10 = """(fe80:((:([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?)(:([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?)?(:([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?)?(:([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?)?)?)?%[\w]+)"""   
  // fe80::7:8%eth0   fe80::7:8%1     (link-local IPv6 addresses with zone index)
  val ipv6Regex11 = """(::((ffff|FFFF)(:00?0?0?)?:)?((25[0-5]|(2[0-4]|1?[0-9])?[0-9])\.)((25[0-5]|(2[0-4]|1?[0-9])?[0-9])\.)?((25[0-5]|(2[0-4]|1?[0-9])?[0-9])\.)?(25[0-5]|(2[0-4]|1?[0-9])?[0-9]))""" 
  // ::255.255.255.255   ::ffff:255.255.255.255  ::ffff:0:255.255.255.255  (IPv4-mapped IPv6 addresses and IPv4-translated addresses)
  val ipv6Regex12 = """([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?([0-9a-fA-F][0-9a-fA-F]?[0-9a-fA-F]?[0-9a-fA-F]?:)?:((25[0-5]|(2[0-4]|1?[0-9])?[0-9])\.)((25[0-5]|(2[0-4]|1?[0-9])?[0-9])\.)((25[0-5]|(2[0-4]|1?[0-9])?[0-9])\.)(25[0-5]|(2[0-4]|1?[0-9])?[0-9])"""
  // 2001:db8:3:4::192.0.2.33  64:ff9b::192.0.2.33 (IPv4-Embedded IPv6 Address)
  val ipv6Regex13 = """(0:0:0:0:0:(0|FFFF|ffff):((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9]))"""
  // 0:0:0:0:0:0:13.1.68.3
  // scalastyle:on
}

case class GpuParseUrl(children: Seq[Expression], 
    failOnErrorOverride: Boolean = SQLConf.get.ansiEnabled) 
  extends GpuExpression with ShimExpression with ExpectsInputTypes {

  def this(children: Seq[Expression]) = this(children, SQLConf.get.ansiEnabled)

  override def nullable: Boolean = true
  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(StringType)
  override def dataType: DataType = StringType
  override def prettyName: String = "parse_url"

  import GpuParseUrl._
  
  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size > 3 || children.size < 2) {
      RapidsErrorUtils.parseUrlWrongNumArgs(children.size) match {
        case res: Some[TypeCheckResult] => return res.get
        case _ => // error message has been thrown
      }
    }
    super[ExpectsInputTypes].checkInputDataTypes()
  }

  private def getPattern(key: UTF8String): RegexProgram = {
    val regex = REGEXPREFIX + key.toString + REGEXSUBFIX
    new RegexProgram(regex)
  }

  private def reValid(url: ColumnVector): ColumnVector = {
    val regex = """([^\s]*\s|([^%[]*|[^%[]*][^%]*)%([^0-9a-fA-F]|[0-9a-fA-F][^0-9a-fA-F]|$))"""
    val prog = new RegexProgram(regex)
    withResource(url.matchesRe(prog)) { isMatch =>
      withResource(Scalar.fromNull(DType.STRING)) { nullScalar =>
        isMatch.ifElse(nullScalar, url)
      }
    }
  }

  private def reMatch(url: ColumnVector, partToExtract: String): ColumnVector = {
    val regex = partToExtract match {
      case HOST => HOST_REGEX
      case PATH => PATH_REGEX
      case QUERY => QUERY_REGEX
      case REF => REF_REGEX
      case PROTOCOL => PROTOCOL_REGEX
      case FILE => FILE_REGEX
      case AUTHORITY => AUTHORITY_REGEX
      case USERINFO => USERINFO_REGEX
      case _ => throw new IllegalArgumentException(s"Invalid partToExtract: $partToExtract")
    }
    val prog = new RegexProgram(regex)
    withResource(url.extractRe(prog)) { table: Table =>
      table.getColumn(0).incRefCount()
    }
  }

  private def uriPathEmptyToNulls(cv: ColumnVector, isUri: ColumnVector): ColumnVector = {
    val res = withResource(ColumnVector.fromStrings("")) { empty =>
      withResource(ColumnVector.fromStrings(null)) { nulls =>
        cv.findAndReplaceAll(empty, nulls)
      }
    }
    withResource(res) { _ =>
      isUri.ifElse(res, cv)
    }
  }

  private def emptyToNulls(cv: ColumnVector): ColumnVector = {
    withResource(ColumnVector.fromStrings("")) { empty =>
      withResource(ColumnVector.fromStrings(null)) { nulls =>
        cv.findAndReplaceAll(empty, nulls)
      }
    }
  }

  private def unsetInvalidHost(cv: ColumnVector): ColumnVector = {
    val regex = "^(" + hostnameRegex + "|" + ipv4Regex + "|" + simpleIpv6Regex + ")$"
    val prog = new RegexProgram(regex)
    val HostnameIpv4Res = withResource(cv.matchesRe(prog)) { isMatch =>
      withResource(Scalar.fromNull(DType.STRING)) { nullScalar =>
        isMatch.ifElse(cv, nullScalar)
      }
    }
    // match the simple ipv6 address, valid ipv6 only when necessary cause the regex is very long
    val simpleIpv6Prog = new RegexProgram(simpleIpv6Regex)
    withResource(cv.matchesRe(simpleIpv6Prog)) { isMatch =>
      val anyIpv6 = withResource(isMatch.any()) { a =>
        a.isValid && a.getBoolean
      }
      if (anyIpv6) {
        withResource(HostnameIpv4Res) { _ =>
          unsetInvalidIpv6Host(HostnameIpv4Res, isMatch)
        }
      } else {
        HostnameIpv4Res
      }
    }
  }

  private def unsetInvalidProtocol(cv: ColumnVector): ColumnVector = {
    val regex = """^[a-zA-Z][\w+\-.]*$"""
    val prog = new RegexProgram(regex)
    withResource(cv.matchesRe(prog)) { isMatch =>
      withResource(Scalar.fromNull(DType.STRING)) { nullScalar =>
        isMatch.ifElse(cv, nullScalar)
      }
    }
  }

  private def unsetInvalidIpv6Host(cv: ColumnVector, simpleMatched: ColumnVector): ColumnVector = {
    val regex = """^\[(""" + ipv6Regex1 + "|" + ipv6Regex2 + "|" + ipv6Regex3 + "|" + ipv6Regex4 + "|" + 
        ipv6Regex5 + "|" + ipv6Regex6 + "|" + ipv6Regex7 + "|" + ipv6Regex8 + "|" + ipv6Regex9 + "|" +
        ipv6Regex10 + "|" + ipv6Regex11 + "|" + ipv6Regex12 + "|" + ipv6Regex13 + """)(%[\w]*)?]$"""
    
    val prog = new RegexProgram(regex)

    val invalidIpv6 = withResource(cv.matchesRe(prog)) { matched =>
      withResource(matched.not()) { invalid =>
        simpleMatched.and(invalid)
      }
    }
    withResource(invalidIpv6) { _ =>
      withResource(Scalar.fromNull(DType.STRING)) { nullScalar =>
        invalidIpv6.ifElse(nullScalar, cv)
      }
    }
  }

  def doColumnar(numRows: Int, url: GpuScalar, partToExtract: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(url, numRows, StringType)) { urlCol =>
      doColumnar(urlCol, partToExtract)
    }
  }

  def doColumnar(url: GpuColumnVector, partToExtract: GpuScalar): ColumnVector = {
    val part = partToExtract.getValue.asInstanceOf[UTF8String].toString
    val valid = reValid(url.getBase)
    val matched = withResource(valid) { _ =>
      reMatch(valid, part)
    }
    if (part == HOST) {
      val valided = withResource(matched) { _ =>
        unsetInvalidHost(matched)
      }
      withResource(valided) { _ =>
        emptyToNulls(valided)
      }
    } else if (part == QUERY || part == REF) {
      val resWithNulls = withResource(matched) { _ =>
        emptyToNulls(matched)
      }
      withResource(resWithNulls) { _ =>
        resWithNulls.substring(1)
      }
    } else if (part == PATH || part == FILE) {
      val isUri = withResource(Scalar.fromString("//")) { doubleslash =>
        withResource(url.getBase.stringContains(doubleslash)) { isurl =>
          isurl.not()
        }
      }
      withResource(isUri) { _ =>
        withResource(matched) { _ =>
          uriPathEmptyToNulls(matched, isUri)
        }
      }
    } else if (part == PROTOCOL) {
      val valided = withResource(matched) { _ =>
        unsetInvalidProtocol(matched)
      }
      withResource(valided) { _ =>
        emptyToNulls(valided)
      }
    } else {
      withResource(matched) { _ =>
        emptyToNulls(matched)
      }
    }
  }

  def doColumnar(url: GpuColumnVector, partToExtract: GpuScalar, key: GpuScalar): ColumnVector = {
    val part = partToExtract.getValue.asInstanceOf[UTF8String].toString
    if (part != QUERY) {
      // return a null columnvector
      return ColumnVector.fromStrings(null, null)
    }
    val querys = withResource(reMatch(url.getBase, QUERY)) { matched =>
      matched.substring(1)
    }
    val keyStr = key.getValue.asInstanceOf[UTF8String]
    val queryValue = withResource(querys) { _ =>
      withResource(querys.extractRe(getPattern(keyStr))) { table: Table =>
        table.getColumn(2).incRefCount()
      }
    }
    withResource(queryValue) { _ =>
      emptyToNulls(queryValue)
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (children.size == 2) {
      val Seq(url, partToExtract) = children
      withResourceIfAllowed(url.columnarEvalAny(batch)) { val0 =>
        withResourceIfAllowed(partToExtract.columnarEvalAny(batch)) { val1 =>
          (val0, val1) match {
            case (v0: GpuColumnVector, v1: GpuScalar) =>
              GpuColumnVector.from(doColumnar(v0, v1), dataType)
            case _ =>
              throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
          }
        }
      }
    } else {
      // 3-arg, i.e. QUERY with key
      assert(children.size == 3)
      val Seq(url, partToExtract, key) = children
      withResourceIfAllowed(url.columnarEvalAny(batch)) { val0 =>
        withResourceIfAllowed(partToExtract.columnarEvalAny(batch)) { val1 =>
          withResourceIfAllowed(key.columnarEvalAny(batch)) { val2 =>
            (val0, val1, val2) match {
              case (v0: GpuColumnVector, v1: GpuScalar, v2: GpuScalar) =>
                GpuColumnVector.from(doColumnar(v0, v1, v2), dataType)
              case _ =>
                throw new 
                    UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
            }
          }
        }
      }
    }
  }
}
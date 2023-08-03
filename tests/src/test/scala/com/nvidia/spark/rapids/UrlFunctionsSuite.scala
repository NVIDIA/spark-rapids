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

package com.nvidia.spark.rapids

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

class UrlFunctionsSuite extends SparkQueryCompareTestSuite {
  def validUrlEdgeCasesDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    // [In search of the perfect URL validation regex](https://mathiasbynens.be/demo/url-regex)
    Seq[String](
      "http://foo.com/blah_blah",
      "http://foo.com/blah_blah/",
      "http://foo.com/blah_blah_(wikipedia)",
      "http://foo.com/blah_blah_(wikipedia)_(again)",
      "http://www.example.com/wpstyle/?p=364",
      "https://www.example.com/foo/?bar=baz&inga=42&quux",
      // "http://✪df.ws/123",
      "http://userid:password@example.com:8080",
      "http://userid:password@example.com:8080/",
      "http://userid:password@example.com",
      "http://userid:password@example.com/",
      "http://142.42.1.1/",
      "http://142.42.1.1:8080/",
      // "http://➡.ws/䨹",
      // "http://⌘.ws",
      // "http://⌘.ws/",
      "http://foo.com/blah_(wikipedia)#cite-1",
      "http://foo.com/blah_(wikipedia)_blah#cite-1",
      // "http://foo.com/unicode_(✪)_in_parens",
      "http://foo.com/(something)?after=parens",
      // "http://☺.damowmow.com/",
      "http://code.google.com/events/#&product=browser",
      "http://j.mp",
      "ftp://foo.bar/baz",
      "http://foo.bar/?q=Test%20URL-encoded%20stuff",
      "http://مثال.إختبار",
      "http://例子.测试",
      "http://उदाहरण.परीक्षा",
      "http://-.~_!$&'()*+,;=:%40:80%2f::::::@example.com",
      "http://1337.net",
      "http://a.b-c.de",
      "http://223.255.255.254",
      // "https://foo_bar.example.com/",
      // "http://",
      // "http://.",
      // "http://..",
      // "http://../",
      "http://?",
      "http://??",
      "http://??/",
      "http://#",
      // "http://##",
      // "http://##/",
      "http://foo.bar?q=Spaces should be encoded",
      "//",
      // "//a",
      // "///a",
      // "///",
      "http:///a",
      // "foo.com",
      "rdar://1234",
      "h://test",
      "http:// shouldfail.com",
      ":// should fail",
      "http://foo.bar/foo(bar)baz quux",
      "ftps://foo.bar/",
      // "http://-error-.invalid/",
      "http://a.b--c.de/",
      // "http://-a.b.co",
      // "http://a.b-.co",
      "http://0.0.0.0",
      "http://10.1.1.0",
      "http://10.1.1.255",
      "http://224.1.1.1",
      // "http://1.1.1.1.1",
      // "http://123.123.123",
      "http://3628126748",
      // "http://.www.foo.bar/",
      "http://www.foo.bar./",
      // "http://.www.foo.bar./",
      "http://10.1.1.1",
      "http://10.1.1.254"
    ).toDF("urls")
  }

  def urlCasesFromSpark(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[String](
      "http://userinfo@spark.apache.org/path?query=1#Ref",
      "https://use%20r:pas%20s@example.com/dir%20/pa%20th.HTML?query=x%20y&q2=2#Ref%20two",
      "http://user:pass@host",
      "http://user:pass@host/",
      "http://user:pass@host/?#",
      "http://user:pass@host/file;param?query;p2"
    ).toDF("urls")
  }

  def urlCasesFromSparkInvalid(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[String](
      "inva lid://user:pass@host/file;param?query;p2"
    ).toDF("urls")
  }

  def urlCasesFromJavaUriLib(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[String](
      "ftp://ftp.is.co.za/rfc/rfc1808.txt",
      "http://www.math.uio.no/faq/compression-faq/part1.html",
      "telnet://melvyl.ucop.edu/",
      "http://www.w3.org/Addressing/",
      "ftp://ds.internic.net/rfc/",
      "http://www.ics.uci.edu/pub/ietf/uri/historical.html#WARNING",
      "http://www.ics.uci.edu/pub/ietf/uri/#Related",
      "http://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:80/index.html",
      "http://[FEDC:BA98:7654:3210:FEDC:BA98:7654:10%12]:80/index.html",
      "http://[1080:0:0:0:8:800:200C:417A]/index.html",
      "http://[1080:0:0:0:8:800:200C:417A%1]/index.html",
      "http://[3ffe:2a00:100:7031::1]",
      "http://[1080::8:800:200C:417A]/foo",
      "http://[::192.9.5.5]/ipng",
      "http://[::192.9.5.5%interface]/ipng",
      "http://[::FFFF:129.144.52.38]:80/index.html",
      "http://[2010:836B:4179::836B:4179]",
      "http://[FF01::101]",
      "http://[::1]",
      "http://[::]",
      "http://[::%hme0]",
      "http://[0:0:0:0:0:0:13.1.68.3]",
      "http://[0:0:0:0:0:FFFF:129.144.52.38]",
      "http://[0:0:0:0:0:FFFF:129.144.52.38%33]",
      "http://[0:0:0:0:0:ffff:1.2.3.4]",
      "http://[::13.1.68.3]"
    ).toDF("urls")
  }

  def urlWithQueryKey(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[String](
      "http://foo.com/blah_blah?foo=bar&baz=blah#vertical-bar"
    ).toDF("urls")
  }

  def utf8UrlCases(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[String](
      "http://user✪info@sp✪ark.apa✪che.org/pa✪th?que✪ry=1#R✪ef",
      "http://@✪df.ws/123",
      "http://@➡.ws/䨹",
      "http://@⌘.ws",
      "http://@⌘.ws/",
      "http://@foo.com/unicode_(✪)_in_parens",
      "http://@☺.damowmow.com/",
      "http://@xxx☺.damowmow.com/",
      "http://@مثال.إختبار/index.html?query=1#Ref",
      "http://@例子.测试/index.html?query=1#Ref",
      "http://@उदाहरण.परीक्षा/index.html?query=1#Ref"
      // "http://user✪info@✪df.ws/123",
      // "http://user✪info@➡.ws/䨹",
      // "http://user✪info@⌘.ws",
      // "http://user✪info@⌘.ws/",
      // "http://user✪info@foo.com/unicode_(✪)_in_parens",
      // "http://user✪info@☺.damowmow.com/",
      // "http://user✪info@xxx☺.damowmow.com/",
      // "http://user✪info@مثال.إختبار/index.html?query=1#Ref",
      // "http://user✪info@例子.测试/index.html?query=1#Ref",
      // "http://user✪info@उदाहरण.परीक्षा/index.html?query=1#Ref"
    ).map(UTF8String.fromString(_).toString()).toDF("urls")
  }

  def unsupportedUrlCases(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[String](
      "https://foo_bar.example.com/",
      "http://",
      "http://.",
      "http://..",
      "http://../",
      "http://##",
      "http://##/",
      "//a",
      "///a",
      "///",
      "foo.com",
      "http://-error-.invalid/",
      "http://-a.b.co",
      "http://a.b-.co",
      "http://1.1.1.1.1",
      "http://123.123.123",
      "http://.www.foo.bar/",
      "http://.www.foo.bar./"
    ).toDF("urls")
  }

  def parseUrls(frame: DataFrame): DataFrame = {
    frame.selectExpr(
      "urls",
      "parse_url(urls, 'HOST') as HOST",
      "parse_url(urls, 'PATH') as PATH",
      "parse_url(urls, 'QUERY') as QUERY",
      "parse_url(urls, 'REF') as REF",
      "parse_url(urls, 'PROTOCOL') as PROTOCOL",
      "parse_url(urls, 'FILE') as FILE",
      "parse_url(urls, 'AUTHORITY') as AUTHORITY",
      "parse_url(urls, 'USERINFO') as USERINFO")
  }

  // def disableGpuRegex(): SparkConf = {
  //   new SparkConf()
  //     .set("spark.rapids.sql.regexp.enabled", "false")
  // }

  testSparkResultsAreEqual("Test parse_url edge cases from internet", validUrlEdgeCasesDf) {
    parseUrls            
  }

  testSparkResultsAreEqual("Test parse_url cases from Spark", urlCasesFromSpark) {
    parseUrls            
  }

  testSparkResultsAreEqual("Test parse_url invalid cases from Spark", urlCasesFromSparkInvalid) {
    parseUrls            
  }

  testSparkResultsAreEqual("Test parse_url cases from java URI library", urlCasesFromJavaUriLib) {
    parseUrls             
  }

  testSparkResultsAreEqual("Test parse_url utf-8 cases", utf8UrlCases) {
    parseUrls             
  }

  // testSparkResultsAreEqual("Test parse_url unsupport cases", unsupportedUrlCases) {
  //   parseUrls
  // }

  testSparkResultsAreEqual("Test parse_url with query and key", urlWithQueryKey) {
    frame => frame.selectExpr(
      "urls",
      "parse_url(urls, 'QUERY', 'foo') as QUERY")
  }
}
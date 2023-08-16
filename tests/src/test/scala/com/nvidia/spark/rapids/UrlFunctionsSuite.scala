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
      "http://✪df.ws/123",
      "http://userid:password@example.com:8080",
      "http://userid:password@example.com:8080/",
      "http://userid:password@example.com",
      "http://userid:password@example.com/",
      "http://142.42.1.1/",
      "http://142.42.1.1:8080/",
      "http://➡.ws/䨹",
      "http://⌘.ws",
      "http://⌘.ws/",
      "http://foo.com/blah_(wikipedia)#cite-1",
      "http://foo.com/blah_(wikipedia)_blah#cite-1",
      "http://foo.com/unicode_(✪)_in_parens",
      "http://foo.com/(something)?after=parens",
      "http://☺.damowmow.com/",
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
      "https://foo_bar.example.com/",
      // "http://",
      "http://.",
      "http://..",
      "http://../",
      "http://?",
      "http://??",
      "http://??/",
      "http://#",
      "http://##",
      "http://##/",
      "http://foo.bar?q=Spaces should be encoded",
      // "//",
      "//a",
      "///a",
      "///",
      "http:///a",
      "foo.com",
      "rdar://1234",
      "h://test",
      "http:// shouldfail.com",
      ":// should fail",
      "http://foo.bar/foo(bar)baz quux",
      "ftps://foo.bar/",
      "http://-error-.invalid/",
      "http://a.b--c.de/",
      "http://-a.b.co",
      "http://a.b-.co",
      "http://0.0.0.0",
      "http://10.1.1.0",
      "http://10.1.1.255",
      "http://224.1.1.1",
      "http://1.1.1.1.1",
      "http://123.123.123",
      "http://3628126748",
      "http://.www.foo.bar/",
      "http://www.foo.bar./", 
      "http://.www.foo.bar./",
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

  def otherEdgeCases(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    // scalastyle:off line.size.limit
    Seq[String](
      "http://fb.com.accounts.login.userid.474878.fbsacc.com/frd/fbn/?next=http%3A%2F%2Fwww.fb.com%2videos%2F%3A%4A%4ID%1A",
      "http://abc.com/a%xx%ueue",
      "áì&ÙêìÏÕ",
      "foo.bar",
      // "",
      "½4+",
      "=Â-",
      "uéò×õA",
      "wc6ËWdþ¨í'",
      ")-",
      "()",
      "foo",
      "123.foo.bar",
      "123.foo.bar:123",
      "foo.bar:123",
      // """http://ecnavi.jp/redirect/?url=http://ad-4091.affit.jp/c.ts/35n.2/-/1g.html?mu=%user_id""",
      """http://ecnavi.jp/redirect/?url=http://ad-4091.affit.jp/c.ts/35n.2/-/1g.html?mu=user_id""",
      """http://www.musimagen.com/lista_socios.php?letra=\303%91""",
      """http://www.fjpengfei.com/picshow.asp?id=31&mnid=5074&classname=\320\373\264\253\273\255\262\341&banner=2""",
      """http://www.musimagen.com/lista_socios.php?letra=%91xx""",
      """http://www.musimagen.com/lista_socios.php?letra=%20xx""",
      """http://www.fjpengfei.com/picshow.asp?id=31&mnid=5074&classname=xxy&banner=2""",
      """http://ecnavi.jp/redirect/?url=**""",
      "http://shtory-g.ru?email=ggradnigo@prepaidlegal.com",
      "http://foo.bar/baduser@xx/yy",
      "http://foo.bar/xx/yy?baduser@zz",
      "http://foo.bar?query=baduser@key",
      "http://foo.bar#baduser@zz",
      "http://foo.bar:666@123/xx/yy",
      "mailto:xx@yy.com",
      "foo.bar/yy?query=key#fragment",       
      "foo.bar:123",
      "foo.bar:123/xx/yy",
      "foo.bar:123/xx/yy?query=key",
      "foo.bar:123/xx/yy/?query=key&query2=key2",
      "foo.bar:123/xx/yy#fragment",
      "foo.bar:123/xx/yy/index.html",
      "foo.bar:123?query=key"
    ).toDF("urls")
  }
  // scalastyle:on

  def urlWithQueryKey(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[String](
      "http://foo.com/blah_blah?foo=bar&baz=blah#vertical-bar"
    ).toDF("urls")
  }

  def urlIpv6Host(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[String](
      "http://[1:2:3:4:5:6:7:8]",
      "http://[1::]",
      "http://[1:2:3:4:5:6:7::]",
      "http://[1::8]",
      "http://[1:2:3:4:5:6::8]",
      "http://[1:2:3:4:5:6::8]",
      "http://[1::7:8]",
      "http://[1:2:3:4:5::7:8]",
      "http://[1:2:3:4:5::8]",
      "http://[1::6:7:8]",
      "http://[1:2:3:4::6:7:8]",
      "http://[1:2:3:4::8]",
      "http://[1::5:6:7:8]",
      "http://[1:2:3::5:6:7:8]",
      "http://[1:2:3::8]",
      "http://[1::4:5:6:7:8]",
      "http://[1:2::4:5:6:7:8]",
      "http://[1:2::8]",
      "http://[1::3:4:5:6:7:8]",
      "http://[1::3:4:5:6:7:8]",
      "http://[1::8]",
      "http://[::2:3:4:5:6:7:8]",
      "http://[::2:3:4:5:6:7:8]",
      "http://[::8]",
      "http://[::]",
      "http://[fe80::7:8%eth0]",
      "http://[fe80::7:8%1]",
      "http://[::255.255.255.255]",
      "http://[::ffff:255.255.255.255]",
      "http://[::ffff:0:255.255.255.255]",
      "http://[2001:db8:3:4::192.0.2.33]",
      "http://[64:ff9b::192.0.2.33]"
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

  testSparkResultsAreEqual("Test parse_url ipv6 host", urlIpv6Host) {
    parseUrls             
  }

  testSparkResultsAreEqual("Test other edge cases", otherEdgeCases) {
    parseUrls
  }

  testSparkResultsAreEqual("Test parse_url with query and key", urlWithQueryKey) {
    frame => frame.selectExpr(
      "urls",
      "parse_url(urls, 'QUERY', 'foo') as QUERY",
      "parse_url(urls, 'QUERY', 'baz') as QUERY")
  }
}
import scala.util.parsing.combinator.{Parsers}

import scala.util.parsing.input.{Reader, PagedSeqReader, StreamReader}

import scala.collection.immutable.PagedSeq

import java.io.InputStreamReader

object GitParser extends Parsers with App {

  type Elem = Char

  var delimiter = ""

  var bytesCount = 0

  def handleWhiteSpace(source: CharSequence, offset: Int): Int = {
    if ((offset > 0) && (source.charAt(offset - 1) != '\n')) {
      offset
    } else if (source.charAt(offset) == '#') {
      var i = 1
      try {
        while (source.charAt(offset + i) != '\n') {
          i += 1
        }
      } catch {
        case ex: IndexOutOfBoundsException => i -= 1
      }
      offset + i
    } else {
      offset
    }
  }

  implicit def literal(s: String): Parser[String] = new Parser[String] {

    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      val start = handleWhiteSpace(source, offset)
      var i = 0
      var j = start
      val buf = new StringBuilder
      while (i < s.length && s.charAt(i) == source.charAt(j)) {
        buf += s.charAt(i)
        i += 1
        j += 1
      }
      if (i == s.length)
        Success(buf.toString, in.drop(j - offset))
      else {
        buf += source.charAt(j)
        Failure("`" + s + "' expected but `" + buf + "' found",
          in.drop(start - offset))
      }
    }
  }

  def repo: Parser[Any] = (
    rep(commit
      | tag
      | reset
      | blob
      | checkpoint
      | progress
      | feature
      | option
    ) ^^ { s => println("repo"); s}
    )

  def commit: Parser[Any] = (
    "commit" ~! SP ~ ref ~ LF
      ~ opt(mark)
      ~ opt("author" ~ opt(SP ~ name) ~ SP ~ LT ~ email ~ GT ~ SP ~ when ~ LF)
      ~ "committer" ~ opt(SP ~ name) ~ SP ~ LT ~ email ~ GT ~ SP ~ when ~ LF
      ~ data
      ~ opt("from" ~ SP ~ committish ~ LF)
      ~ opt("merge" ~ SP ~ committish ~ LF)
      ~ opt(rep(fileModify | fileDelete | fileCopy | fileRename | fileDeleteAll
      | noteModify)) ^^ { s => println("commit"); s}
    )

  def tag: Parser[Any] = (
    "tag" ~! SP ~ name ~ LF
      ~ "from" ~ SP ~ committish ~ LF
      ~ "tagger" ~ opt(SP ~ name) ~ SP ~ LT ~ email ~ GT ~ SP ~ when ~ LF
      ~ data ^^ { s => println("tag"); s }
    )

  def reset: Parser[Any] = (
    "reset" ~! SP ~ ref ~ LF
      ~ opt("from" ~ SP ~ committish ~ LF)
      ~ opt(LF)
    )

  def blob: Parser[String] = (
    "blob" ~> LF
      ~> opt(mark)
      ~> data ^^ { (s: String) => println("blob"); s }
    )

  def feature: Parser[Any] = "feature" ~! SP ~ letterSeq ~ LF

  def option: Parser[Any] = "option" ~! SP ~ charSeq ~ LF

  def checkpoint: Parser[Any] = "checkpoint" ~! LF ~ opt(LF)

  def progress: Parser[Any] = "progress" ~! SP ~ any ~ LF ~ opt(LF)

  def ref: Parser[String] = charSeq

  def mark: Parser[Any] = "mark" ~ SP ~ markref ~ LF ^^ { s => println("mark"); s}

  def markref: Parser[String] = ":" ~> idnum ^^ { s => val v = ":" + s; println("markref " + v); v}

  def idnum: Parser[Int] = numRead

  def name: Parser[String] = charSeq

  def email: Parser[String] = charSeq

  def when: Parser[String] = charSeq

  def data: Parser[String] =
    "data" ~> SP ~> dataBody

  def dataBody: Parser[String] = exactByteCountFormat | delimitedFormat

  def exactByteCountFormat: Parser[String] =
    count ~> LF ~> rawBytes <~ opt(LF)

  def delimitedFormat: Parser[String] =
    "<<" ~> delim ~> LF ~> rawDelimited <~ LF <~ delim <~ LF <~ opt(LF)

  def SP: Parser[String] = " "

  def LF: Parser[String] = "\n"

  def LT: Parser[String] = "<"

  def GT: Parser[String] = ">"

  def count = numRead ^^ { (n: Int) => bytesCount = n; bytesCount}

  def numRead: Parser[Int] = new Parser[Int] {

    def apply(in: Input) = {
      println("numRead")
      val source = in.source
      val offset = in.offset
      var i = 0
      var num = 0
      while (source.charAt(offset + i).isDigit) {
        println(source.charAt(offset + i))
        num *= 10
        num += (source.charAt(offset + i) - '0')
        i += 1
      }
      i match {
        case 0 => Failure("integer count expected but found", in.drop(i))
        case _ => { Success(num, in.drop(i)) }
      }
    }
  }

  def rawBytes: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
      println("rawBytes")
      val source = in.source
      val offset = in.offset
      val buf = new StringBuilder
      var i = 0
      while (i < bytesCount) {
        buf += source.charAt(offset + i)
        i += 1
      }
      Success(buf.toString(), in.drop(bytesCount))
    }
  }


  def rawDelimited: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
      println("rawDelimited")
      val source = in.source
      val offset = in.offset
      var i = 0
      val buf = new StringBuilder
      while (!buf.endsWith(delimiter)) {
        buf += source.charAt(offset + i)
        i += 1
      }
      if (i >= delimiter.length)
        Success(buf.toString, in.drop(i))
      else {
        Failure("expected delimiter found`" + buf + "'", in)
      }
    }
  }

  def delim: Parser[String] = charSeq ^^ {
    (s: String) => delimiter = s; s
  }

  def committish: Parser[Any] = (":" ~ mark) | charSeq

  def fileModify: Parser[Any] = "M" ~ SP ~ mode ~ SP ~ fileBody

  def fileDelete: Parser[Any] = "D" ~ SP ~ path ~ LF

  def fileCopy: Parser[Any] = "C" ~ SP ~ path ~ SP ~ path ~ LF

  def fileRename: Parser[Any] = "R" ~ SP ~ path ~ SP ~ LF

  def fileDeleteAll: Parser[Any] = "deleteall" ~ LF

  def noteModify: Parser[Any] = "N" ~ SP ~ noteModifyBody

  def noteModifyBody: Parser[Any] = externalDataFormat | inlineDataFormat

  def mode: Parser[String] = (
    "100644"
      | "644"
      | "100755"
      | "755"
      | "120000"
      | "160000"
    )

  def fileBody: Parser[Any] = externalDataFormat | inlineDataFormat

  def externalDataFormat: Parser[Any] = dataref ~ SP ~ path ~ LF

  def inlineDataFormat: Parser[String] =
    "inline" <~ SP <~ path <~ LF <~ data

  def dataref: Parser[Any] = markref | charSeq

  def path: Parser[String] = charSeq

  def any: Parser[String] = charSeq

  def charSeq: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
      println("charSeq")
      val source = in.source
      val offset = in.offset
      var i = 0
      val buf = new StringBuilder
      while (source.charAt(offset + i) != '\n') {
        buf += source.charAt(offset + i)
        i += 1
      }
      if (i > 0)
        Success(buf.toString, in.drop(i))
      else {
        Failure("expected character sequence found newline", in)
      }
    }
  }

  def letterSeq: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
      println("letterSeq")
      val source = in.source
      val offset = in.offset
      var i = 0
      val buf = new StringBuilder
      while (source.charAt(offset + i).isLetter) {
        buf += source.charAt(offset + i)
        i += 1
      }
      if (i > 0)
        Success(buf.toString, in.drop(i))
      else {
        Failure("expected letter sequence found `" + buf + "'", in)
      }
    }
  }

  
  val in: Reader[Char] =
    if (args.length > 0) {
      val isr = scala.io.Source.fromFile(args(0))
      StreamReader(new java.io.FileReader(args(0)))
      //new PagedSeqReader(PagedSeq.fromSource(isr))
    } else {
      val isr = new InputStreamReader(System.in)
      new PagedSeqReader(PagedSeq.fromReader(isr))
    }

  println("Started parsing")
  phrase(repo)(in)
  println(in.offset)
  println("Finished parsing")
  //println(parse(repo, in))
}
  

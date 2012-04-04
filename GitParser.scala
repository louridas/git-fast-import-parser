import scala.util.parsing.combinator.{Parsers}

import scala.util.parsing.input.{Reader, PagedSeqReader, StreamReader}

import scala.collection.immutable.PagedSeq

import java.io.InputStreamReader

trait GitActionsCallbacks {

  def commitCallback(ref: String, mark: Option[String], author: Option[String], committer: String,  data: String,
                     from: Option[String],  merge: Option[String],
                     fileNoteChanges: Option[List[String]])

  def resetCallback(ref: String, committish: Option[String]): Tuple2[String, Option[String]]

  def blobCallback(mark: Option[String],  data: String): Tuple2[Option[String], String]
}


class GitParser(var gitActionCallbacks: GitActionsCallbacks = null) extends Parsers with GitActionsCallbacks {

  type Elem = Char

  var delimiter = ""

  var lineNum = 1

  var charsRead = 0

  var bytesCount = 0

  var numCommits = 0

  var numBlobs = 0

  var charBuffer = new StringBuilder

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
      if (i == s.length) {
        Success(buf.toString, in.drop(j - offset))
      } else {
        buf += source.charAt(j)
        Failure(lineNum + ": `" + s + "' expected but `" + buf + "' found", in.drop(start - offset))
      }
    }
  }

  def repo: Parser[Any] = rep(action)
      
  def action: Parser[Any] = (
    commit
    | tag
    | reset
    | blob
    | checkpoint
    | progress
    | feature
    | option
  )

  def commit: Parser[Any] = (
    "commit" ~! SP ~ ref ~ LF
      ~ opt(mark)
      ~ opt("author" ~ opt(SP ~ personName) ~ SP ~ LT ~ email ~ GT ~ SP ~ when ~ LF)
      ~ "committer" ~ opt(SP ~ personName) ~ SP ~ LT ~ email ~ GT ~ SP ~ when ~ LF
      ~ data
      ~ opt("from" ~ SP ~ committish ~ LF)
      ~ opt("merge" ~ SP ~ committish ~ LF)
      ~ opt(rep(fileModify | fileDelete | fileCopy | fileRename | fileDeleteAll
      | noteModify))
      ~ opt(LF) ^^ {
      case "commit" ~ _ ~ r ~ _ ~ m ~ a ~
        "committer" ~ committerName ~ _ ~ _ ~ committerEmail ~ _ ~ _ ~ committerWhen ~ _
        ~ d ~ f ~ mg ~ fn ~ _ => {
        val ac = a match {
          case t @ Some("author" ~ authorName ~ _ ~ _ ~ authorEmail ~  _ ~ _ ~ authorWhen ~ _) =>
            Some("author %s <%s> %s".format(authorName, authorEmail, authorWhen))
          case _ => None
        }
        val committerString = "committer %s <%s> %s".format(committerName, committerEmail, committerWhen)
        val fc = f match {
          case Some("from" ~ _ ~ c ~ _) => Some(c)
          case None => None
        }
        val mgc = mg match {
          case Some("merge" ~ _ ~ c ~ _) => Some(c)
          case None => None
        }
        commitCallback(r, m, ac, committerString, d, fc, mgc, fn)
      }
    }
    )

  def tag: Parser[Any] = (
    "tag" ~! SP ~ name ~ LF
      ~ "from" ~ SP ~ committish ~ LF
      ~ "tagger" ~ opt(SP ~ name) ~ SP ~ LT ~ email ~ GT ~ SP ~ when ~ LF
      ~ data
    )

  def reset: Parser[Any] = (
    "reset" ~! SP ~ ref ~ LF
      ~ opt("from" ~ SP ~ committish ~ LF)
      ~ opt(LF) ^^ {
        case "reset" ~ _ ~ r ~ _ ~ f ~ _ => {
          f match {
            case Some("from" ~ _ ~ c ~ _) => resetCallback(r, Some(c))
            case None => resetCallback(r, None)
          }
        }
      }
    )

  def blob: Parser[(Option[String], String)] = (
    "blob" ~ LF ~ opt(mark) ~ data ^^ { case "blob" ~ _ ~ m ~ d => blobCallback(m, d) }
    )

  def feature: Parser[Any] = "feature" ~! SP ~ letterSeq ~ LF

  def option: Parser[Any] = "option" ~! SP ~ charSeq ~ LF

  def checkpoint: Parser[Any] = "checkpoint" ~! LF ~ opt(LF)

  def progress: Parser[Any] = "progress" ~! SP ~ any ~ LF ~ opt(LF)

  def ref: Parser[String] = charSeq

  def mark: Parser[String] = "mark" ~ SP ~ markref ~ LF ^^ { case "mark" ~ _ ~ m ~ _ => m }

  def markref: Parser[String] = ":" ~> idnum ^^ { (d: Int) => ":" + d }

  def idnum: Parser[Int] = numRead

  def personName: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      var i = 0
      val buf = new StringBuilder
      while (source.charAt(offset + i + 1) != '<') {
        buf += source.charAt(offset + i)
        i += 1
      }
      if (i > 0)
        Success(buf.toString, in.drop(i))
      else {
        Failure(lineNum + ": expected person name but found `" + buf + "'" , in)
      }
    }
  }

  def email: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      var i = 0
      val buf = new StringBuilder
      while (source.charAt(offset + i) != '>') {
        buf += source.charAt(offset + i)
        i += 1
      }
      if (i > 0)
        Success(buf.toString, in.drop(i))
      else {
        Failure(lineNum + ": expected email but found `" + buf + "'" , in)
      }
    }
  }

  def when: Parser[String] = charSeq

  def name: Parser[String] = charSeq

  def data: Parser[String] = "data" ~ SP ~ dataBody ^^ { case "data" ~ _ ~ db => db }

  def dataBody: Parser[String] = exactByteCountFormat | delimitedFormat

  def exactByteCountFormat: Parser[String] =
    count ~ LF ~ rawBytes ~ opt(LF) ^^ { case c ~ _ ~ rb ~ _ => rb }

  def delimitedFormat: Parser[String] =
    "<<" ~> delim ~> LF ~> rawDelimited <~ LF <~ delim <~ LF <~ opt(LF)

  def SP: Parser[String] = " "

  def LF: Parser[String] = "\n" ^^ { (s: String) => lineNum += 1; s }

  def LT: Parser[String] = "<"

  def GT: Parser[String] = ">"

  def count = numRead ^^ { (n: Int) => bytesCount = n; bytesCount}

  def numRead: Parser[Int] = new Parser[Int] {

    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      var i = 0
      var num = 0
      while (source.charAt(offset + i).isDigit) {
        num *= 10
        num += (source.charAt(offset + i) - '0')
        i += 1
      }
      i match {
        case 0 => Failure(lineNum + ": integer count expected but found", in.drop(i))
        case _ => { Success(num, in.drop(i)) }
      }
    }
  }

  def rawBytes: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      var i = 0
      charBuffer = new StringBuilder()
      while (i < bytesCount) {
        val ch = source.charAt(offset + i)
        if (ch == '\n') lineNum += 1
        charBuffer += ch
        i += 1
      }
      Success("rawBytes", in.drop(bytesCount))
    }
  }


  def rawDelimited: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      var i = 0
      charBuffer = new StringBuilder()
      while (!charBuffer.endsWith(delimiter)) {
        val ch = source.charAt(offset + i)
        if (ch == '\n') lineNum += 1
        charBuffer += source.charAt(ch)
        i += 1
      }
      if (i >= delimiter.length)
        Success("rawDelimited", in.drop(i))
      else {
        Failure(lineNum + ": expected delimiter but found`" + charBuffer + "'", in)
      }
    }
  }

  def delim: Parser[String] = charSeq ^^ {
    (s: String) => delimiter = s; s
  }

  def committish: Parser[String] = (":" ~ mark) ^^ { case ":" ~ m => m } | charSeq

  def fileModify: Parser[String] = "M" ~ SP ~ mode ~ SP ~ fileBody ^^ {
    case "M" ~ _ ~ m ~ _ ~ b => "M %s %s".format(m, b)
  }

  def fileDelete: Parser[String] = "D" ~> SP ~> path <~ LF  ^^ { (p: String) => "D %s".format(p) }

  def fileCopy: Parser[String] = "C" ~ SP ~ path ~ SP ~ path ~ LF ^^ {
    case "C" ~ _ ~ p1 ~ _ ~ p2 ~ _ => "C %s %s".format(p1, p2)
  }

  def fileRename: Parser[String] = "R" ~> SP ~> path <~ SP <~ LF ^^ { (p: String) => "R % s".format(p) }

  def fileDeleteAll: Parser[String] = "deleteall" ~ LF ^^ { case "deleteall" ~ _ => "deleteall" }

  def noteModify: Parser[String] = "N" ~> SP ~> noteModifyBody ^^ { (n: String) => "N %s".format(n) }

  def noteModifyBody: Parser[String] = externalDataFormat | inlineDataFormat

  def mode: Parser[String] = (
    "100644"
      | "644"
      | "100755"
      | "755"
      | "120000"
      | "160000"
    )

  def fileBody: Parser[String] = externalDataFormat | inlineDataFormat

  def externalDataFormat: Parser[String] = dataref ~ SP ~ path ~ LF ^^ {
    case d ~ _ ~ p ~ _ => "%s %s".format(d, p)
  }

  def inlineDataFormat: Parser[String] = "inline" ~ SP ~ path ~ LF ~ data ^^ {
    case "inline" ~ _ ~ p ~ _ ~ d => "inline %s\n%d".format(p, d)
  }

  def dataref: Parser[String] = markref | charSeq

  def path: Parser[String] = charSeq

  def any: Parser[String] = charSeq

  def charSeq: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
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
        Failure(lineNum + ": expected character sequence but found newline", in)
      }
    }
  }

  def letterSeq: Parser[String] = new Parser[String] {

    def apply(in: Input) = {
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
        Failure(lineNum + ": expected letter sequence but found `" + buf + "'", in)
      }
    }
  }

  /*
   Callback functions
   */

  def commitCallback(ref: String, mark: Option[String], author: Option[String], committer: String,  data: String,
                     from: Option[String],  merge: Option[String],
                     fileNoteChanges: Option[List[String]]) = {
    numCommits += 1
    if (gitActionCallbacks != null) {
      gitActionCallbacks.commitCallback(ref, mark, author, committer, data, from, merge, fileNoteChanges)
    } else {
      (ref, mark, author, committer, data, from, merge, fileNoteChanges)
    }
  }

  def resetCallback(ref: String, committish: Option[String]): Tuple2[String, Option[String]] = {

    if (gitActionCallbacks != null) {
      gitActionCallbacks.resetCallback(ref, committish)
    } else {
      (ref, committish)
    }
  }

  def blobCallback(mark: Option[String],  data: String): Tuple2[Option[String], String] = {
    numBlobs += 1
    if (gitActionCallbacks != null) {
      gitActionCallbacks.blobCallback(mark, data)
    } else {
      (mark, data)
    }
  }

  def parseAction(in: Reader[Char]): (Reader[Char], Boolean) = {
    val result = action(in)
    if (result.successful) {
      (result.next, true)
    } else {
      println(result.toString())
      (result.next, false)
    }
  }
}

class GitPagedSeqReader(seq: PagedSeq[Char], override val offset: Int) extends PagedSeqReader(seq, offset) {

  def fromHere: GitPagedSeqReader = new GitPagedSeqReader(seq, offset)
}

object GitParser {

  def main(args: Array[String]) = {

    var in: Reader[Char] =
      if (args.length > 0) {
        StreamReader(new java.io.FileReader(args(0)))
      } else {
        val isr = new InputStreamReader(System.in)
        new PagedSeqReader(PagedSeq.fromReader(isr))
      }

    println("Starting parsing")
    val start = System.nanoTime
    var parsedOK = true
    val heapMaxSize = Runtime.getRuntime.maxMemory
    val gitParser = new GitParser
    while (!in.atEnd && parsedOK) {
      gitParser.action(in) match {
        case f: gitParser.Failure => { println(f.msg); parsedOK = false }
        case s @ _ => in = s.next
      }
      if (Runtime.getRuntime.totalMemory() + 10000 > heapMaxSize) {
        System.gc
      }
    }
    val end = System.nanoTime

    println("Number of commits: " + gitParser.numCommits)
    println("Finished parsing, took " + (end - start) + " nano seconds")
  }
}
  

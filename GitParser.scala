import scala.util.parsing.combinator.RegexParsers

import scala.util.parsing.input.{Reader, PagedSeqReader}

import scala.collection.immutable.PagedSeq

import java.io.InputStreamReader

import scala.util.matching.Regex;
  
class CharSequenceWithOffset private[CharSequenceWithOffset] (val underlying: CharSequence, val offset: Int) extends CharSequence {  
  
  def charAt(index: Int): Char = underlying.charAt(offset + index)   
  
  def length: Int = underlying.length - offset  
  
  def subSequence(start: Int, end: Int): CharSequence = 
    underlying.subSequence(offset + start, offset + end)  
  
  override def toString: String = 
    underlying.subSequence(offset, underlying.length).toString 

  }

object CharSequenceWithOffset {  
  def apply(underlying: CharSequence, offset: Int) = underlying match {
    case orig : CharSequenceWithOffset => 
      new CharSequenceWithOffset(orig.underlying, offset + orig.offset)    
    case _ => new CharSequenceWithOffset(underlying, offset)  
  }
} 
 
object GitParser extends RegexParsers with App {

  /** A parser that matches a regex string */
  override implicit def regex(r: Regex): Parser[String] = new Parser[String] {
    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      val start = handleWhiteSpace(source, offset)
      val cso = CharSequenceWithOffset(source, offset)
      (r.findPrefixMatchOf(cso)) match {
	case Some(matched) =>
	  Success(source.subSequence(start, start + matched.end).toString, 
	          in.drop(start + matched.end - offset))
	case None =>
	  val found = if (start == source.length()) "end of source" else "`"+source.charAt(start)+"'" 
	Failure("string matching regex `"+r+"' expected but "+found+" found", in.drop(start - offset))
      }
    }
  }
  
  override val whiteSpace = "^#.*$".r
  
  override def handleWhiteSpace(source: java.lang.CharSequence, 
                                offset: Int): Int = {
    if (skipWhitespace) {
      val cso = CharSequenceWithOffset(source, offset)
      whiteSpace.findPrefixMatchOf(cso) match {
        case Some(matched) => offset + matched.end
        case None => offset
      }
    } else {
      offset
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
      ) 
  )
    
  def commit: Parser[Any] = (
    "commit"~!SP~ref~LF
    ~opt(mark)
    ~opt("author"~opt(SP~name)~SP~LT~email~GT~SP~when~LF)
    ~"committer"~opt(SP~name)~SP~LT~email~GT~SP~when~LF
    ~data    
    ~opt("from"~SP~committish~LF)
    ~opt("merge"~SP~committish~LF)
    ~opt(rep(fileModify | fileDelete | fileCopy | fileRename | fileDeleteAll
             | noteModify)) 
  )

  def tag: Parser[Any] = (
    "tag"~!SP~name~LF
    ~"from"~SP~committish~LF
    ~"tagger"~opt(SP~name)~SP~LT~email~GT~SP~when~LF
    ~data
  )
  
  def reset: Parser[Any] = (
    "reset"~!SP~ref~LF
    ~opt("from"~SP~committish~LF)
    ~opt(LF)
  )

  def blob: Parser[String] = (
    "blob"~>LF
    ~>opt(mark)
    ~>data
    
  )
  
  def feature: Parser[Any] = "feature"~!SP~"""^[a-zA-Z]+""".r~LF
  
  def option: Parser[Any] = "option"~!SP~".+".r~LF
  
  def checkpoint: Parser[Any] = "checkpoint"~!LF~opt(LF)
  
  def progress: Parser[Any] = "progress"~!SP~any~LF~opt(LF)
    
  def ref: Parser[String] = ".+".r
  
  def mark: Parser[Any] = "mark"~SP~markref~LF
  
  def markref: Parser[Any] = ":"~idnum
  
  def idnum: Parser[String] = """\d+""".r
  
  def name: Parser[String] = ".+".r
  
  def email: Parser[String] = ".+".r
  
  def when: Parser[String] = ".+".r
  
  def data: Parser[String] = 
    "data"~>SP~>dataBody 
  
  def dataBody: Parser[String] = exactByteCountFormat | delimitedFormat
  
  def exactByteCountFormat: Parser[String] = 
    count~>LF~>raw<~opt(LF)
  
  def delimitedFormat: Parser[String] = 
    "<<"~>delim~>LF~>raw<~LF<~delim<~LF<~opt(LF)
  
  def SP: Parser[String] = " "
    
  def LF: Parser[String] = "\n"
  
  def LT: Parser[String] = "<"
  
  def GT: Parser[String] = ">"
  
  def count: Parser[String] = """\d+""".r
  
  def raw: Parser[String] = "(?s).*".r
  
  def delim: Parser[String] = ".+".r
  
  def committish: Parser[Any] = (":"~mark) | ".+".r
  
  def fileModify: Parser[Any] = "M"~SP~mode~SP~fileBody

  def fileDelete: Parser[Any] = "D"~SP~path~LF
  
  def fileCopy: Parser[Any] = "C"~SP~path~SP~path~LF
  
  def fileRename: Parser[Any] = "R"~SP~path~SP~LF
  
  def fileDeleteAll: Parser[Any] = "deleteall"~LF
  
  def noteModify: Parser[Any] = "N"~SP~noteModifyBody
  
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
  
  def externalDataFormat: Parser[Any] = dataref~SP~path~LF
  
  def inlineDataFormat: Parser[String] = 
    "inline"<~SP<~path<~LF<~data 
  
  def dataref: Parser[Any] = markref | ".+".r
  
  def path: Parser[String] = ".+".r
  
  def any: Parser[String] = """[^\n]*""".r
  
  val in: Reader[Char] = 
    if (args.length > 0) {
      val isr = scala.io.Source.fromFile(args(0))
      new PagedSeqReader(PagedSeq.fromSource(isr))
    } else {
      val isr = new InputStreamReader(System.in)
      new PagedSeqReader(PagedSeq.fromReader(isr))
    }
  println(parseAll(repo, in))
}
  

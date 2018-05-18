package org.jayawelch.utils

import java.io.FileReader

import scala.util.{Failure, Success, Try}

/**
  * Describes the elements of a 'section' of an XML document that is to be divided/chunked.
  *
  * @param startElem String the label name of the enclosing element where the children nodes
  *                  reside. e.g. "<records>"
  * @param endElem  String The label name of the enclosing element where the children nodes
  *                 reside. e.g. "</records>"
  * @param startAndEndSubElements List[String] The names/labels of the end-elements that are to be
  *                               divided. e.g. List("</record>",...)
  * @param pace Int The number of Characters that are to be read in each time. Should be large to read
  *             in at least one node at a time
  */
case class XMLSection(startElem:String,endElem:String,startAndEndSubElements:List[String],pace:Int)

/**
  * The current state of where in the xml section the process has been.
  *
  * @param data Option[xml.NodeSeq] If the end-elements can be found and the data combined with the
  *             XMLSection's startElem and endElem for a valid xml document then this value will be
  *             Some(xml.NodeSeq) otherwise None
  *
  * @param input  java.io.Reader Holds the current position in the xml document that is being read. Used
  *               to read the characters by the given pace.
  *
  * @param leftover String The portion of the last read that is not a complete node. Will be combined
  *                 with the next read to form the next xml.NodeSeq
  * @param finished Boolean Indicates if the endElem of the XMLSection has been read or not.
  */
case class XMLState(data:Option[xml.NodeSeq], input:java.io.Reader, leftover:String,finished:Boolean)

object XMLChunk
{
  /**
    *
    *  @return Stream[Either[Throwable,xml.NodeSeq] ] If for any circumstance there is a problem
    *         during the reading of the given file then this will immediately return a Throwable
    *         upon the next cycle and terminate the stream. If there is no problem during the reading
    *         of each cycle then the appropriate xml.NodeSeq will be returned and at the end of the section
    *         the stream will be properly terminated.
    *
    * @param enclosingElementName String The name of the element to search where to begin within the document. All the
    *                             sub-elements underneath this element will be streamed in smaller chunks. For every
    *                             chunk this will be the parent-element.
    *
    * @param endElements These are the names of the all the sub-elements that could possibly be within the given
    *                    enclosing element.
    *
    * @param pace Int How fast(how many bytes to read) should this stream process each time.
    *
    * @param file java.io.File The xml file that contains the section to stream.
    *
    *
    */
  def apply(enclosingElementName:String,endElements:List[String],pace:Int,file:java.io.File):Stream[Either[Throwable,xml.NodeSeq]] = {
    val fr = new FileReader(file)
    val section = XMLSection("<"+enclosingElementName+">","</"+enclosingElementName+">",endElements.map("</"+_+">"),pace)
    def loop(state:Try[XMLState]):Stream[Either[Throwable,xml.NodeSeq]] =
      state match {
        case Failure(throwable) => {
          Try(fr.close)
          Left(throwable) #:: Stream.empty[Either[Throwable, xml.NodeSeq]]
        }
        case Success(xmlState) => xmlState match {
          case XMLState(None,reader,_,_) => {
            Try(reader.close())
            Left(new RuntimeException("No data!. Maybe the pace is too small or the specified end elements are not correct.")) #:: Stream.empty[Either[Throwable,xml.NodeSeq]]
          }
          case XMLState(Some(data),reader,_,true) => {
            Try(reader.close())
            Right(data) #:: Stream.empty[Either[Throwable,xml.NodeSeq]]
          }
          case XMLState(Some(data),reader,_,false) => {
            Right(data) #:: loop(next(section,xmlState))
          }
        }
      }
    loop(start(enclosingElementName,section,fr))
  }


  /**
    *
    * @param section XMLSection describes the section to find. Looking for the startElem.
    *
    * @param input java.io.Reader General reader used to read the characters.
    *
    * @return Try[XMLState] If for some reason there is an io exception raised or
    *         some of the elements could not be found that are specified in the given section then
    *         a Throwable will be returned otherwise an XMLState will be returned with the first
    *         xml element(s) enclosed.
    */
  def start(enclosingElement:String,section:XMLSection,input:java.io.Reader):Try[XMLState] = {
    def loop(s:String,state:XMLState):Try[XMLState] = {
      val ndx = s.indexOf("<"+enclosingElement)
      if (ndx != -1) {
        val endNdx = s.indexOf(">",ndx+enclosingElement.length)
        divide(s.substring(endNdx+1), section.startAndEndSubElements) match {
          case None => Failure(new RuntimeException("Unable to find either of the " + section.startAndEndSubElements + " end element(s)."))
          case Some((data, leftOver)) => {
            Try(xml.XML.loadString(section.startElem + data + section.endElem))
              .flatMap(elem => Success(XMLState(Some(elem), state.input, leftOver, state.finished)))
          }
        }
      } else {
        state match {
          case XMLState(_,reader,_,false) => {
            val data = new Array[Char](section.pace)
            Try(reader.read(data)).flatMap(numRead => {
                val (good,noGood) = if(numRead != section.pace) data.splitAt(numRead) else (data,Array.empty[Char])
                val s = new String(good)
                loop(s,XMLState(None,reader,"",s.lastIndexOf(section.endElem) != -1))
              })
            }
          case XMLState(_,_,_,true) => Failure(new RuntimeException("Did not find the start(" + section.startElem + ") in the given input"))
        }
      }
    }
    loop("",XMLState(None,input,"",false))
  }

  /**
    *
    * @param section XMLSection Describes the section of the xml source to read
    *
    * @param state XMLState The current state of the divying up of the xml by section
    *
    * @return Try[XMLState] If there is an io exception raised while reading
    *         or an exception thrown while loading the XML from the just read data then those
    *         exception will be returned. Otherwise the new data will be read and transformed
    *         into an xml.NodeSeq using the given Section start and end elements.
    */
  def next(section:XMLSection,state:XMLState):Try[XMLState] = {
    val data = new Array[Char](section.pace)
    Try(state.input.read(data)).flatMap(numRead => {
      if(numRead != -1) {
        val (real,non) = if(numRead != section.pace) data.splitAt(numRead) else (data,Array.empty[Char])
        divide(state.leftover + new String(real),section.startAndEndSubElements) match {
          case Some((data,leftOver)) => {
            val sb = new StringBuilder
            sb.append(section.startElem)
            sb.append(data)
            sb.append(section.endElem)
            Try(xml.XML.loadString(sb.toString())) match {
              case Success(dataSeq) =>  Success(XMLState(Some(dataSeq),state.input,leftOver,leftOver.lastIndexOf(section.endElem) != -1))
              case Failure(t) => Failure(t)
            }
          }
          case None => Failure(new RuntimeException("Unable to find the end element(s) " + section.startAndEndSubElements + " within the last batch of data."))
        }
      } else
        Success(XMLState(None,state.input,"",true))
    })
  }

  /**
    * <elem1>...</elem1><elem2>....</elem2><elem1>...</elem1>
    *
    * @param s String a "chunk" of xml data that will be divided by one of the given endElements.
    *
    * @param endElements List[String] the end elements ("</...>","</...>") that should be defined in the given
    *                    String s.
    *
    * @return Option[(String,String)] If any of the given end elements can be found within the given string s,
    *         then Some(...tuple...) will be returned where the first element is the part of the string that contains
    *         the last end element specified in the given endElements container. The second element is the remaining
    *         part of s that does not have a end element value in the given string. If no end elements are found then
    *         None will be returned.
    */
  def divide(s:String,endElements:List[String]):Option[(String,String)] = {
    val (ndx,endElem) = endElements.map(elem => (s.lastIndexOf(elem),elem)).maxBy(_._1)
    if(ndx != -1)
      Some((s.substring(0,ndx+endElem.length),s.substring(ndx+endElem.length)))
    else
      None
  }


}

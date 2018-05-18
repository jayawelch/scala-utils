package org.jayawelch.utils

import org.scalatest.FunSuite

class XMLChunkTest extends FunSuite
{
  test("divide") {
    val s = "<test1 id=\"1\"><blah></blah></test1><test2 id=\"2\"><blah></blah></test2><otherElem><boo><fac"
    val endElements = List("</test1>","</test2>")
    val div1 = XMLChunk.divide(s,endElements)
    assert(div1.isDefined)
    val (data1,leftOver1) = div1.get
    assert(data1 == "<test1 id=\"1\"><blah></blah></test1><test2 id=\"2\"><blah></blah></test2>" &&
           leftOver1 == "<otherElem><boo><fac")
    val div2 = XMLChunk.divide(s,endElements.reverse)
    assert(div2.isDefined)
    val (data2,leftOver2) = div2.get
    assert(data2 == "<test1 id=\"1\"><blah></blah></test1><test2 id=\"2\"><blah></blah></test2>" &&
      leftOver2 == "<otherElem><boo><fac")
  }

  test("next") {
    val testSection = XMLSection("<records>","</records>",List("</record>"),1000)
    val sr = new java.io.StringReader("<record id=\"1\"><data><names></names></data></record><record id=\"2\"><data><names></names></data></record><recor")
    val result = XMLChunk.next(testSection,XMLState(None,sr,"",false))
    if(result.isFailure)
      println(result.failed.get.getLocalizedMessage)
    assert(result.isSuccess)
    val resultState = result.get
    //println(resultState)
    assert(resultState match {
      case XMLState(Some(data),_,"<recor",false) => {
         data == <records><record id="1"><data><names/></data></record><record id="2"><data><names/></data></record></records>
      }
      case _ => false
    })
  }

  test("start") {
    val recordSection = XMLSection("<records>","</records>",List("</record>"),1000)
    val sr = new java.io.StringReader("<definitions><definition id=\"1\"/><definition id=\"2\"/><definitions></definitions><records><record id=\"1\"><data><names></names></data></record><record id=\"2\"><data><names></names></data></record></records><associations><associate id=\"1\"></association><associate id=\"2\"></associate></associations>")
    val recordResult = XMLChunk.start("records",recordSection,sr)
    assert(recordResult.isSuccess)
    val recordStartState = recordResult.get
    assert(recordStartState match {
      case XMLState(Some(data),reader,_,true) => {
        data == <records><record id="1"><data><names/></data></record><record id="2"><data><names/></data></record></records>
      }
      case _ => false
    })

    val definitionSection = XMLSection("<definitions>","</definitions>",List("</definition>"),100)
    val definitionResult = XMLChunk.start("definitions",definitionSection,
                            new java.io.StringReader("<definitions><definition id=\"1\"></definition><definition id=\"2\"></definition></definitions><records><record id=\"1\"><data><names></names></data></record><record id=\"2\"><data><names></names></data></record></records><associations><associate id=\"1\"></association><associate id=\"2\"></associate></associations>"))

    assert(definitionResult.isSuccess)
    val definitionState = definitionResult.get
    assert(definitionState match {
      case XMLState(Some(data),_,_,true) => {
        data == <definitions><definition id="1"/><definition id="2"/></definitions>
      }
      case _ => false
    })

    val associationSection = XMLSection("<associations>","</associations>",List("</associate>"),100)
    val associationResult = XMLChunk.start("associations",associationSection,
                                          new java.io.StringReader("<definitions><definition id=\"1\"></definition><definition id=\"2\"></definition></definitions><records><record id=\"1\"><data><names></names></data></record><record id=\"2\"><data><names></names></data></record></records><associations><associate id=\"1\"></associate><associate id=\"2\"></associate></associations>"))
    assert(associationResult.isSuccess)
    val associationState = associationResult.get
    assert(associationState match {
      case XMLState(Some(data),_,s,false) => {
        s.nonEmpty && data == <associations><associate id="1"/><associate id="2"/></associations>
      }
      case XMLState(Some(data),_,_,true) => {
        data == <associations><associate id="1"/><associate id="2"/></associations>
      }
      case _ => false
    })

  }

  test("chunk-stream") {
    val booksURL = this.getClass.getResource("/books.xml")

    def loop(chunk:Stream[Either[Throwable,xml.NodeSeq]],map:Map[String,Seq[(String,String)]]):Either[Throwable,Map[String,Seq[(String,String)]]] = {
      if(chunk.isEmpty)
        Right(map)
      else {
        val h = chunk.head
        if(h.isRight) {
          val (m1,m2) = (h.right.get \\ "book").groupBy(node => {
            (node \\ "genre").head.text
          }).map(kv =>  kv._1 -> kv._2.map(bookNode =>
            ((bookNode \\ "title").head.text,(bookNode \\ "author").head.text)
          )).partition(kv => map.contains(kv._1))
          loop(chunk.tail,map.map(kv => kv._1 -> (if(m1.contains(kv._1)) kv._2.++:(m1(kv._1)) else kv._2)) ++ m2)
        } else
          Left(h.left.get)
      }
    }

    val books = loop(XMLChunk("catalog",List("book"),500,new java.io.File(booksURL.getFile)),Map.empty[String,Seq[(String,String)]])
    assert(books.isRight)
    val bookMap = books.right.get
    assert(Set("Horror","Computer","Romance","Science Fiction","Fantasy").diff(bookMap.keySet).isEmpty)
    assert(Seq(("Creepy Crawlies","Knorr, Stefan")).diff(bookMap("Horror")).isEmpty)
    assert(Seq(("Visual Studio 7: A Comprehensive Guide","Galos, Mike"),("MSXML3: A Comprehensive Guide","O'Brien, Tim"),
               ("Microsoft .NET: The Programming Bible","O'Brien, Tim"),("XML Developer's Guide","Gambardella, Matthew"))
            .intersect(bookMap("Computer")).size == bookMap("Computer").size)
    assert(Seq(("Splish Splash","Thurman, Paula"),("Lover Birds","Randall, Cynthia"))
              .intersect(bookMap("Romance")).size == bookMap("Romance").size)
    assert(Seq(("Paradox Lost","Kress, Peter")).intersect(bookMap("Science Fiction")).size == bookMap("Science Fiction").size)
    assert(Seq(("Oberon's Legacy","Corets, Eva"),("The Sundered Grail","Corets, Eva"),
               ("Maeve Ascendant","Corets, Eva"),("Midnight Rain","Ralls, Kim"))
               .intersect(bookMap("Fantasy")).size == bookMap("Fantasy").size)
  }

  test("chunk-stream-failure") {
    val booksURL = this.getClass.getResource("/books.xml")

    val stream = XMLChunk("catalog",List("boook"),500,new java.io.File(booksURL.getFile))
    assert(stream.head.isLeft && stream.tail.isEmpty)
  }


  /*test("big file") {

    def loop(strm:Stream[Either[Throwable,xml.NodeSeq]],total:Int):Int =
      if(strm.isEmpty)
        total
      else if(strm.head.isRight)
        loop(strm.tail,total+(strm.head.right.get \\ "ProteinEntry").size)
      else {
        println(strm.head.left.get.getLocalizedMessage)
        -1
      }

    println(loop(XMLChunk("ProteinDatabase",
                         List("ProteinEntry"),
                         15*1024*1014,
                         new java.io.File("/home/jayw/data/psd7003.xml")),0))

  }*/

}

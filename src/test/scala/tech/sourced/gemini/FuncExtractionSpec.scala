package tech.sourced.gemini

import org.bblfsh.client.BblfshClient
import org.scalatest.{FlatSpec, Matchers}

@tags.Bblfsh
class FuncExtractionSpec extends FlatSpec
  with Matchers {

  val client = BblfshClient("localhost", 9432)

  case class File(name: String, content: String, numberOfFunctions: Int)

  val fixtures = List(
    File("a.go",
      """
        |package p
        |func A() {}
        |func B() {}
      """.stripMargin, 2),
    File("b.go",
      """
        |package p
        |func A() { a := func() {}}
        |func B() {}
      """.stripMargin, 2),
    File("c.py", //only one function is detect here by alg in Python :/
      """
        |class A:
        |    def a(self):
        |        class B:
        |            def b(self):
        |                pass
      """.stripMargin, 1)
  )

  "extractFunctions" should "extract all known-good functions from UAST" in {
    fixtures.foreach { fixture =>
      val resp = client.parse(fixture.name, fixture.content)
      if (!resp.errors.isEmpty) {
        fail(s"failed to parse ${fixture.name}: ${resp.errors.mkString("\n\t")}")
      }
      resp.uast.foreach { uast =>
        val functions = Hash.extractFunctions(uast)
        functions.size shouldEqual fixture.numberOfFunctions

        functions.foreach { case (fnName, fnUast) =>
          println(s"${fixture.name}_${fnName}:${fnUast.getStartPosition.line}")
        }
      }
    }
  }

}

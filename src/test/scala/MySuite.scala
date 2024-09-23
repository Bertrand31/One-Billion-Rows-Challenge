// For more information on writing tests, see
import onebrc.processAndSort
import java.nio.charset.StandardCharsets
import scala.io.Source

class AllSpec extends munit.FunSuite {
  test("should print expected data") {

    val res = processAndSort()
    val expected = Source.fromResource("output.txt").getLines().mkString

    // StringBuilger seems to make a sparse string, with lots of 0s
    val denseRes = res.toCharArray.filterNot(_ == 0).mkString
    assertEquals(denseRes.size, expected.size)
    assertEquals(denseRes, expected)
  }
}

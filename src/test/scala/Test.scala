import scala.collection.JavaConverters.asJavaIterableConverter

import com.lannister.core.domain.HeuristicResultDetail
import com.lannister.core.util.Utils


object Test {
  def main(args: Array[String]): Unit = {
    val list = List[HeuristicResultDetail](HeuristicResultDetail("A", "1")).asJava
    // scalastyle:off println
    val str = Utils.toJson(list)
    println(str)


  }
}

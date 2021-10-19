import com.lannister.core.domain.HeuristicResultDetail
import com.lannister.core.util.Utils


object Test {
  def main(args: Array[String]): Unit = {
    val list = List[HeuristicResultDetail](HeuristicResultDetail("A", "1"))


    val map = list.map(hd => hd.name -> hd.value).toMap
    // scalastyle:off println
    val str = Utils.toJson(map)
    println(str)


  }
}

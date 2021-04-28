

object Test {


  def main(args: Array[String]): Unit = {
    for{i <- 1 to 3
      a=3} {
      println(" i =" + i )
//      println(" i =" + i + " j = " + j)

    }



    for{i <- 1 to 3
        j <- 1 to 3} {

      println(" i =" + i + " j = " + j)

    }
  }
}

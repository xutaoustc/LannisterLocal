import scala.concurrent.duration.{Duration, MINUTES}

object Test {


  def main(args: Array[String]): Unit = {

    val cc = Duration(5, MINUTES).toMillis


    for{x<-None
        y<-Option(3)}{
      print(s"${x} ss ${y}")
    }

  }
}

package sessions

object pattern_matching {

  def main(args:Array[String])=
  {
    println("Hello world")
    val x = 5
    val y = x match {
      case 5 => "I'm five"
      case _ => "I'm not five"
    }

    val x1:Any = "10.0"
    val y1:Any = x1 match {
      case a:Int => "I'm Integer"
      case a:Double => "I'm Double"
      case b:String => "I'm String"
    }
    println( s" value of y1 is ${y1}")


    val x2 = 8
    val y2 = x2 match {
      case p if p%2==0 => "Even"
      case p if p%2!=0 => "Odd"
    }

    println(s"Value of y2 is ${y2}")
  }
}

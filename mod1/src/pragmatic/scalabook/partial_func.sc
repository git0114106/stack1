val a = List(10,20,30,40)
a.isDefinedAt(5)
val indexes= Seq(0,1,2,40)
indexes collect a

a(3)
//a(4)
a.lift(4)

val divide5: PartialFunction[Int, Int] = {
  case d: Int => 42 / d
}

divide5.isDefinedAt(10)
divide5.isDefinedAt(0)

val convertNumberToString = new PartialFunction[Int,String] {
  val nums = List("one","two","three","four","five")
  def apply(x:Int) = nums(x+1)
  def isDefinedAt(x:Int) = x < 5
}

if( convertNumberToString.isDefinedAt(3) ) convertNumberToString.isDefinedAt(3)
if( convertNumberToString.isDefinedAt(7) ) convertNumberToString.isDefinedAt(7)


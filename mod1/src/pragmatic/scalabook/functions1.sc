

val a = List("decimal","string", "decimal","string","integer")
a.toSet

// String Versions
def removeDuplicates_str(xs:List[String]):List[String]=
{
  if (xs.isEmpty)
  xs
  else
  xs.head :: removeDuplicates_str(xs.tail.filter( x => x!= xs.head ))
}

removeDuplicates_str(a)

def removeDuplicates[A](xs:List[A]):List[A]={
  if(xs.isEmpty) xs
  else
    xs.head :: removeDuplicates(xs.tail.filter( x => x!= xs.head))
}

removeDuplicates(a)
removeDuplicates(List(10,20,10,40,50,20))

val in = new java.util.Scanner( new java.net.URL("http://horstmann.com/presentations/livelessons-scala-2016/alice30.txt").openStream)
val count =  scala.collection.mutable.Map[String,Int]()

while( in.hasNext){
  val word = in.next()
  count(word) = count.getOrElse(word,0)+1
}

count("Alice")
count("Rabbit")

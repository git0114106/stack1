
def incrementValOfKeys(acc: Map[String, Int], keys: String*) = keys.foldLeft(acc)(
  (acc, key) => acc + (key -> (acc(key) + 1))
)

def incrementValOfKeys2(acc: Map[String, Int], keys: String*) = Map("total" -> 1)

val a = List("decimal","string","string","decimal","timestamp","decimal", "timestamp" )
a.foldLeft(Map[String,Int]().withDefaultValue(0)) (incrementValOfKeys(_, _))

incrementValOfKeys2(Map("abc"->10))
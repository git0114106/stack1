val month = "August"
val quarter = month match {
  case "January" | "February" | "March"    => "1st quarter"
  case "April" | "May" | "June"            => "2nd quarter"
  case "July" | "August" | "September"     => "3rd quarter"
  case "October" | "November" | "December" => "4th quarter"
  case _                                   => "unknown quarter"
}

println(quarter)


object DayOfWeek extends Enumeration {
  val SUNDAY = Value("Sunday")
  val MONDAY = Value("Monday")
  val TUESDAY = Value("Tuesday")
  val WEDNESDAY = Value("Wednesday")
  val THURSDAY = Value("Thursday")
  val FRIDAY = Value("Friday")
  val SATURDAY = Value("Saturday")
}

def activity(day: DayOfWeek.Value) {
  day match {
    case DayOfWeek.SUNDAY => println("Eat, sleep, repeat...")
    case DayOfWeek.SATURDAY => println("Hang out with friends")
    case _ => println("...code for fun...")
  }
}

activity(DayOfWeek.SATURDAY)
activity(DayOfWeek.MONDAY)

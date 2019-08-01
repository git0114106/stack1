package pragmatic.scalabook
import scala.io.Source._
object TopStock {


  def getYearEndClosingPrice(symbol: String, year: Int)=
  {
        val url2 =  s"http://finance.yahoo.com/chart/table.csv?s="+
    s"$symbol&a=11&b=01&c=$year&d=11&e=31&f=$year&g=m"
    val url = "https://finance.yahoo.com/quote/MSFT/history?period1=1463461200&period2=1494910800&interval=1d&filter=history&frequency=1d"
    val data = fromURL(url).mkString
    println(data)
    //val price = data.split( "\n")(1).split(",")(4).toDouble
    //price
    data
  }
  def main(args:Array[String]) =
  {
      val symbols = List("AMD","AAPL","AMZN","IBM","ORCL","MSFT")
    val year = 2014
        val (topStock, topPrice) =    symbols.map { ticker => ( ticker, getYearEndClosingPrice(ticker,year))}
                                               .maxBy(stockPrice => stockPrice._2 )
      println("Hello world")

  }
}

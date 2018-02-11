import org.apache.spark.sql.Column

val categoryMappings = List(
  List("t01", "t03", "t11", "t1801", "t1803"),
  List("t05", "t1805"),
  List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")
).zipWithIndex

val columnNames = List("t1805","t1811","t1803","t05")

val test = categoryMappings.flatMap(m => if(m._1.exists(e => e.startsWith("t01"))) Some(("t01", m._2)) else None)

val test2 = categoryMappings.flatMap(
  {case (names, idx) if names.exists(n => "t1801".startsWith(n)) => Some(("t1801", idx))
  case _ => None}).sortBy(_._2).headOption
columnNames.foldLeft(List.empty[(Int, Column)])((acc, name)=>{
  (1, new Column(name)) :: acc
})


val groups: Map[Int, List[Column]] = columnNames
  .foldLeft(List.empty[(Int, Column)])((acc, name) => {
    categoryMappings
      .flatMap {
        case (prefixes, index) if prefixes.exists(name.startsWith) => Some((index, new Column(name)))
        case _ => None
      }
      .sortBy(_._1)
      .headOption match {
      case Some(tuple) => tuple :: acc
      case None => acc
    }
  }
  )
  .groupBy(_._1)
  .mapValues(_.map(_._2))


columnNames
  .foldLeft(List.empty[(Int, Column)])((acc, name) => {
    categoryMappings
      .flatMap {
        case (prefixes, index) if prefixes.exists(name.startsWith) => Some((index, new Column(name)))
        case _ => None
      }
      .sortBy(_._1)
      .headOption match {
      case Some(tuple) => tuple :: acc
      case None => acc
    }
  }
  )
  .groupBy(_._1)


val name="t1805"


categoryMappings
  .flatMap {
    case (prefixes, index) if prefixes.exists(name.startsWith) => Some((index, new Column(name)))
    case _ => None
  }
.sortBy(_._1)


val listOfOptions = categoryMappings
  .map {
    case (prefixes, index) if prefixes.exists(name.startsWith) => Some((index, new Column(name)))
    case _ => None
  }

listOfOptions.flatten

listOfOptions.flatMap(o=>o)

List(List("one"), List(1), List(1000)).zipWithIndex


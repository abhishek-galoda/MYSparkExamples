package marvel


import org.apache.spark.SparkContext

object SuperHero {

  def mostPopularHero(sc: SparkContext): Unit = {

    val marvelApperances = "src/main/resources/DataSet/Marvel-graph.txt"
    val marvelNames = "src/main/resources/DataSet/Marvel-names.txt"

    //Extract the Hero ID and number of connections from each Line
    val appearences = sc.textFile(marvelApperances).map {
      line => {
        val split = line.split(" ")
        (split(0).toInt, split.length - 1)
      }
    }
    //Extract the total number of appearances
    val totalAppearnces = appearences.reduceByKey((x, y) => x + y)

    val maxAppearances = totalAppearnces.map(x => (x._2, x._1)).max()

    val names = sc.textFile(marvelNames).flatMap(parseNames)

    val mostPopularHero = names.lookup(maxAppearances._2)(0)

    println(s"$mostPopularHero is the most popular superhero with ${maxAppearances._1} co-appearances.")

  }

  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String): Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
}

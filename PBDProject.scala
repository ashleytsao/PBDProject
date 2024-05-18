import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
// Basic Statistical Analysis of Sentiment
// Sentiment Distribution: Calculate the count of each sentiment category (positive, negative) to understand the distribution
// Sentiment Ratios: Calculate the percentage of each sentiment category.

object PBDProject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("PBD Project").getOrCreate()

    val datasetPath = "hw6_dir/part-r-00000"
    val data = spark.read.option("inferSchema", "true").option("header", "true").csv(datasetPath)
    val renamedData = data.toDF("sentiment", "tweet")

    // Convert the 'sentiment' column to a numeric type, e.g., Integer
    val df = renamedData.withColumn("sentiment", col("sentiment").cast("integer"))

    // Displaying the initial data
    df.show(5)

    //Counts of Sentiments
    val totalCount = df.agg(count("sentiment")).first().getLong(0)
    println(s"Total count: $totalCount")

    val countZero = df.filter($"sentiment" === 0).count()
    val zeroPercentage = (countZero.toDouble / totalCount) * 100
    println(s"Total count of sentiments with feeling as 0 (negative): $countZero")
    println(s"Percentage of sentiments with feeling as 0 (negative): $zeroPercentage")
    
    val countOne = df.filter($"sentiment" === 1).count()
    val onePercentage = (countOne.toDouble / totalCount) * 100
    println(s"Total count of sentiments with feeling as 1 (positive): $countOne")
    println(s"Percentage of sentiments with feeling as 1 (positive): $onePercentage")

    spark.stop()
  }
}

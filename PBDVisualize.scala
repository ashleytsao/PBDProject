import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
// Basic Statistical Analysis of Sentiment
// Sentiment Distribution: Calculate the count of each sentiment category (positive, negative) to understand the distribution
// Sentiment Ratios: Calculate the percentage of each sentiment category.

object PBDVisualize {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("PBD Visualize").getOrCreate()

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

    // Save data for visualization
    val sentimentCounts = df.groupBy("sentiment").count()
    val total = sentimentCounts.agg(sum("count")).as("total").first().getLong(0)
    val sentimentPercentages = sentimentCounts.withColumn("percentage", col("count") / total * 100)

    // Writing the counts and percentages to CSV files
    sentimentCounts.write.format("csv").option("header", "true").save("sentiment_counts.csv")
    sentimentPercentages.write.format("csv").option("header", "true").save("sentiment_percentages.csv")

    spark.stop()
  }
}

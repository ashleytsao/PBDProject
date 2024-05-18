import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FirstCode {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Data Analysis and Cleaning").getOrCreate()

    val datasetPath = "hw6_dir/part-r-00000"
    val data = spark.read.option("inferSchema", "true").option("header", "true").csv(datasetPath)
    val renamedData = data.toDF("sentiment", "tweet")

    // Convert the 'sentiment' column to a numeric type, e.g., Integer
    val df = renamedData.withColumn("sentiment", col("sentiment").cast("integer"))

    // Displaying the initial data
    df.show(5)

    // Statistical Analysis
    // Mean, Median (approxQuantile), Mode and Standard Deviation of `sentiment`
    val meanVal = df.agg(mean("sentiment")).first().getDouble(0)
    println(s"Mean sentiment: $meanVal")

    val medianVal = df.stat.approxQuantile("sentiment", Array(0.5), 0.0).head
    println(s"Median sentiment: $medianVal")

    val modeVal = df.groupBy("sentiment").count().orderBy(desc("count")).first()(0)
    println(s"Mode sentiment: $modeVal")

    val stdDevVal = df.agg(stddev("sentiment")).first().getDouble(0)
    println(s"Standard Deviation of sentiment: $stdDevVal")

    // Data Cleaning Options
    // Text formatting: removing extra spaces and making all lowercase
    val cleanedDf = df.withColumn("tweet", lower(trim(col("tweet"))))

    // Creating a binary column based on `sentiment` > 2
    val finalDf = cleanedDf.withColumn("positive_sentiment", when(col("sentiment") > 2, lit(1)).otherwise(lit(0)))

    // Show the cleaned DataFrame
    finalDf.show(5)

    spark.stop()
  }
}

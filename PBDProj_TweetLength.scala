import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PBDProj_TweetLength {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("PBD Project").getOrCreate()

    // Load the dataset assuming it is plain text with a single column
    val datasetPath = "Proj_tweetLength_dir/part-r-00000"
    val data = spark.read.text(datasetPath).toDF("line")

    // Assume the format is "feeling-word count" and split it
    val splitData = data.withColumn("split_col", split(col("line"), ","))
    val finalData = splitData.withColumn("feeling_tweet", col("split_col").getItem(0)).withColumn("tweetLength", col("split_col").getItem(1).cast("integer")).drop("line", "split_col")

    // Displaying the transformed data
    println("First 50 for Both Feelings")
    finalData.show(50)

    // Filter and sort data for negative feelings
    val filteredDF0 = finalData.filter(expr("substring(feeling_tweet, length(feeling_tweet), 1) = '0'"))
    println("First 50 in 0, negative feeling")
    filteredDF0.show(50)

    println("Statistics in 0, negative feeling")
    val meanVal0 = filteredDF0.agg(mean("tweetLength")).first().getDouble(0)
    println(s"Mean length in 0, negative feeling: $meanVal0")
    val medianVal0 = filteredDF0.stat.approxQuantile("tweetLength", Array(0.5), 0.0).head
    println(s"Median length in 0, negative feeling: $medianVal0")
    val modeVal0 = filteredDF0.groupBy("tweetLength").count().orderBy(desc("count")).first()(0)
    println(s"Mode length in 0, negative feeling: $modeVal0")
    val stdDevVal0 = filteredDF0.agg(stddev("tweetLength")).first().getDouble(0)
    println(s"Standard Deviation of length in 0, negative feeling: $stdDevVal0")
    println()

    // Filter and sort data for positive feelings
    val filteredDF1 = finalData.filter(expr("substring(feeling_tweet, length(feeling_tweet), 1) = '1'"))
    println("First 50 in 1, positive feeling")
    filteredDF1.show(50)

    println("Statistics in 0, negative feeling")
    val meanVal1 = filteredDF1.agg(mean("tweetLength")).first().getDouble(0)
    println(s"Mean length in 1, positive feeling: $meanVal1")
    val medianVal1 = filteredDF0.stat.approxQuantile("tweetLength", Array(0.5), 0.0).head
    println(s"Median length in 1, positive feeling: $medianVal1")
    val modeVal1 = filteredDF1.groupBy("tweetLength").count().orderBy(desc("count")).first()(0)
    println(s"Mode length in 1, positive feeling: $modeVal1")
    val stdDevVal1 = filteredDF1.agg(stddev("tweetLength")).first().getDouble(0)
    println(s"Standard Deviation of length in 1, positive feeling: $stdDevVal1")


    // Stop the Spark session
    spark.stop()
  }
}

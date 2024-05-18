import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PBDProj_WordFreq {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("PBD Project").getOrCreate()

    // Load the dataset assuming it is plain text with a single column
    val datasetPath = "Proj_textAnalysis_dir/part-r-00000"
    val data = spark.read.text(datasetPath).toDF("line")

    val stopWords = Set("a", "about", "above", "after", "again", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how", "i", "if", "in", "into", "is", "it", "its", "itself", "just", "me", "more", "most", "my", "myself", "no", "nor", "not", "now", "of", "off", "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own", "s", "same", "she", "should", "so", "some", "such", "t", "than", "that", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "you", "your", "yours", "yourself", "yourselves")

    // Assume the format is "feeling-word count" and split it
    val splitData = data.withColumn("split_col", split(col("line"), ","))
    val finalData = splitData.withColumn("feeling_word", col("split_col").getItem(0)).withColumn("wordCount", col("split_col").getItem(1).cast("integer")).drop("line", "split_col")

    // Displaying the transformed data
    val rearrangeFinal = finalData.orderBy(col("wordCount").desc)
    println("Top 50 Overall")
    rearrangeFinal.show(50)

    // Filter and sort data for negative feelings
    val df0 = finalData.filter(expr("substring(feeling_word, length(feeling_word), 1) = '0'"))
    val cleandf0 = df0.withColumn("feeling_word", expr("substring(feeling_word, 1, length(feeling_word) - 2)"))
    val filteredDF0 = cleandf0.filter(!col("feeling_word").isin(stopWords.toSeq: _*))
    val rearrangedDF0 = filteredDF0.orderBy(col("wordCount").desc)
    println("Top 50 most common words in 0, negative feeling")
    rearrangedDF0.show(50)

    // Filter and sort data for positive feelings
    val df1 = finalData.filter(expr("substring(feeling_word, length(feeling_word), 1) = '1'"))
    val cleandf1 = df1.withColumn("feeling_word", expr("substring(feeling_word, 1, length(feeling_word) - 2)"))
    val filteredDF1 = cleandf1.filter(!col("feeling_word").isin(stopWords.toSeq: _*))
    val rearrangedDF1 = filteredDF1.orderBy(col("wordCount").desc)
    println("Top 50 most common words in 1, positive feeling")
    rearrangedDF1.show(50)

    // Stop the Spark session
    spark.stop()
  }
}

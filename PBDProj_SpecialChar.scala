import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PBDProj_SpecialChar {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("PBD Project").getOrCreate()

    // Load the dataset assuming it is plain text with a single column
    val datasetPath = "Proj_textAnalysis_dir/part-r-00000"
    val data = spark.read.text(datasetPath).toDF("line")

    // Assume the format is "feeling-word count" and split it
    val splitData = data.withColumn("split_col", split(col("line"), ","))
    val finalData = splitData.withColumn("feeling_word", col("split_col").getItem(0)).withColumn("wordCount", col("split_col").getItem(1).cast("integer")).drop("line", "split_col")

    val symbolLabelData = finalData.withColumn("symbol", when(col("feeling_word").startsWith("#"), "#")
                                                .when(col("feeling_word").startsWith("@"), "@")
                                                .when(col("feeling_word").startsWith("$"), "$")
                                                .when(col("feeling_word").startsWith("^"), "^")
                                                .otherwise("None"))
    
    val SymbolData = symbolLabelData.filter($"symbol" === "#" || $"symbol" === "@" || $"symbol" === "$" || $"symbol" === "^")
    println("Words beginning with symbols")
    SymbolData.show(50)



    val df0 = SymbolData.filter(expr("substring(feeling_word, length(feeling_word), 1) = '0'"))
    println("Words beginning with symbols in 0, negative sentiment")
    df0.show(50)
    
    // Filter and count for each symbol
    val hashtags0 = df0.filter($"symbol" === "#").agg(sum("wordCount")).first().getLong(0)
    val mentions0 = df0.filter($"symbol" === "@").agg(sum("wordCount")).first().getLong(0)
    val dollar0 = df0.filter($"symbol" === "$").agg(sum("wordCount")).first().getLong(0)
    val caret0 = df0.filter($"symbol" === "^").agg(sum("wordCount")).first().getLong(0)

    println("Words beginning with # in 0, negative sentiment:")
    df0.filter($"symbol" === "#").show(50)
    println("Number of words beginning with # in 0, negative sentiment: " + hashtags0)

    println("Words beginning with @ in 0, negative sentiment:")
    df0.filter($"symbol" === "@").show(50)
    println("Number of words beginning with @ in 0, negative sentiment: " + mentions0)

    println("Words beginning with $ in 0, negative sentiment:")
    df0.filter($"symbol" === "$").show(50)
    println("Number of words beginning with $ in 0, negative sentiment: " + dollar0)

    println("Words beginning with ^ in 0, negative sentiment:")
    df0.filter($"symbol" === "^").show(50)
    println("Number of words beginning with ^ in 0, negative sentiment: " + caret0)



    val df1 = SymbolData.filter(expr("substring(feeling_word, length(feeling_word), 1) = '1'"))
    println("Words beginning with symbols in 1, positive sentiment")
    df1.show(50)


    // Filter and count for each symbol
    val hashtags1 = df1.filter($"symbol" === "#").agg(sum("wordCount")).first().getLong(0)
    val mentions1 = df1.filter($"symbol" === "@").agg(sum("wordCount")).first().getLong(0)
    val dollar1 = df1.filter($"symbol" === "$").agg(sum("wordCount")).first().getLong(0)
    val caret1 = df1.filter($"symbol" === "^").agg(sum("wordCount")).first().getLong(0)

    println("Words beginning with # in 1, positive sentiment:")
    df1.filter($"symbol" === "#").show(50)
    println("Number of words beginning with # in 1, positive sentiment: " + hashtags1)

    println("Words beginning with @ in 1, positive sentiment:")
    df1.filter($"symbol" === "@").show(50)
    println("Number of words beginning with @ in 1, positive sentiment: " + mentions1)

    println("Words beginning with $ in 1, positive sentiment:")
    df1.filter($"symbol" === "$").show(50)
    println("Number of words beginning with $ in 1, positive sentiment: " + dollar1)

    println("Words beginning with ^ in 1, positive sentiment:")
    df1.filter($"symbol" === "^").show(50)
    println("Number of words beginning with ^ in 1, positive sentiment: " + caret1)


    // Stop the Spark session
    spark.stop()
  }
}

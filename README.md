# PBDProject

1. Data profiling
   a. I first explored the data and only unique values were kept
   b. My code was built to output the number of records (add a column that has the count of the number of records.)
   c. Names of files BEFORE CLEANING: UniqueRecs.java, UniqueRecsMapper.java, UniqueRecsReducer.java
     i. With 3 columns
   d. Names of files AFTER CLEANING:  UniqueRecs2.java, UniqueRecsMapper2.java, UniqueRecsReducer2.java
     ii. After cleaning, I found that only 2 columns were necessary for my project
   e. Output: hw6_dir (cleaned and unique data is stored here)

2. Data Cleaning
   a. This step was done in between parts c and d of step 1 (data profiling)
   b. The names of my three cleaning files – Clean.java, CleanMapper.java, CleanReducer.java
   c. This MR job cleans the data: this is done by dropping unneeded columns and dropping rows if they are NULL

3. Initial Code Analysis
   a. This was my initial code analysis for my dataset
   b. I found the mean, median, mode, and standard deviation of sentiment value
   c. I did text formatting (removing spaces, extra characters, making all caps or all lowercase for normalization / future joining)
   d. I created a binary column based on `sentiment` > 2
   e. The file is called FirstCode.Scala and was ran in Spark

4. Basic Statistical Analysis of Sentiment
   a. I did a sentiment distribution and found the sentiment ratios
     i. Sentiment Distribution: I calculated the count of each sentiment category (positive, negative) to understand the distribution.
     ii. Sentiment Ratios: Calculate the ratio or percentage of each sentiment category.
     iii. The name of the file is – PBDProject.scala
     iv. Input data: hw6_dir/part-r-00000
     v. Output data: run in spark (file – screenshots/Basic Statistical Analysis)
   b. How to run: data_ingest → Commands to run Basic Statistical Analysis

5. Text Analysis and Word Frequencies
   a. I analyzed the text content to find common words, phrases, or sentiment indicators. This provided insights into the most frequent terms associated with different sentiment categories.
   b. Word Count: I split the tweet text into words and counted their frequencies.
     i. Used a MapReduce job
     ii. Names of MapReduce files – TextAnalysis.java, TextAnalysisCleaner.java, TextAnalysisReducer.java
     iii. Input: hw6_dir/part-r-00000
     iv. Output: Proj_textAnalysis_dir
       1. Screenshots are in screenshots/wordCount
       2. Each word with frequency is displayed
     v. How to run: data_ingest → Commands to run WordCount
   c. Word Frequency: Identify common words in each sentiment category to understand what terms are most associated with positive or negative sentiments.
     i. File name – PBDProj_WordFreq.scala
     ii. Input: hw6_dir/part-r-00000
     iii. Output: in Spark (screenshots/wordFreq)
     iv. Created stop words to eliminate common words such as “to” , “the” , “a” , etc.
     v. How to run: data_ingest → Commands to run WordFreq

6. Tweet Content Analysis
   a. I analyzed characteristics of the tweet text that could be related to sentiment
   b. Tweet Length Distribution: I determined if there's a correlation between tweet length and sentiment.
     i. I did two things for this: ran a MapReduce job and then used Scala to interpret the results
     ii. MapReduce file names – TweetLength.java, TweetLengthMapper.java, TweetLengthReducer.java
       1. MapReduce job outputted each tweet and its the length
       2. Input: hw6_dir/part-r-00000
       3. Output: Proj_tweetLength_dir
           a. Screenshots in screenshots/tweetLength/MR
           b. Each sentiment and its character length
       4. How to run: data_ingest → Commands to run TweetLength MR
     iii. Scala file name – PBDProj_TweetLength.scala
       1. Run on spark to find statistics on tweet length (mean, median, mode, standard deviation for both positive and negative individually)
       2. Screenshots in screenshots/tweetLength/Scala
       3. How to run: data_ingest → Commands to run TweetLength Scala
  c. Special Characters and Symbols: Checked for the presence of specific symbols and their relation to sentiment.
     i. Checked for words beginning with these 4 symbols: #, $, @, ^
     ii. Counted the number of each symbol in relation to their sentiment
     iii. File name – PBDProj_SpecialChar 
     iv. Input: Proj_textAnalysis_dir/part-r-00000
     v. Output: run in Spark
        1. Screenshots in screenshots/specialChar
     vi. How to run: data_ingest → Commands to run specialChar

INPUT DATA: SentimentAnalysisDataset.csv located in input_data folder also uploaded to HPC and shared





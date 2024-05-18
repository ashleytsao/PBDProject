import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] columns = line.split("\t");
    if (columns.length >= 2) {
      String feeling = columns[0].substring(0, 1);
      String sentiment = columns[1];
      String[] words = sentiment.split("\\s+"); // Split the sentiment into words
      for (String word : words) {
        String outputKey = word + "-" + feeling + ",";
        context.write(new Text(outputKey), new IntWritable(1)); // Emit (word, 1) for each word
      }
    }

  }
}
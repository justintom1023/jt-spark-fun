package justintom1023.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import justintom1023.mapper.LineMapper;

public class WordCount {

	public void start() {
		
		String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" + 
			  		"'for', 'if', 'in', 'into', 'is', 'it',\r\n" + 
			  		"'no', 'not', 'of', 'on', 'or', 'such',\r\n" + 
			  		"'that', 'the', 'their', 'then', 'there', 'these',\r\n" + 
			  		"'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," + 
			  		"'your', 'you', 'I', "
			  		+ " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";
		 
		SparkSession spark = SparkSession.builder()
		        .appName("Unstructured Text to Flatmap")
		        .master("local")
		        .getOrCreate();
		
		String filename = "src/main/resources/shakespeare.txt";
		
		Dataset<Row> df = spark.read().format("text")
		        .load(filename);
		
		Dataset<String> wordsDS = df.flatMap(new LineMapper(), Encoders.STRING());
		
		Dataset<Row> df2 = wordsDS.toDF();
		
		df2 = df2.groupBy("value").count();
		df2 = df2.orderBy(df2.col("count").desc());
		df2 = df2.filter("LOWER(value) NOT IN " + boringWords);
		
		df2.show(20);
		
	}
	
}

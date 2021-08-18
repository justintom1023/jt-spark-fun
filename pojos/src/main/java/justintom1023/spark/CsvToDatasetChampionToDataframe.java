package justintom1023.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import justintom1023.mapper.ChampionMapper;
import justintom1023.pojos.Champion;

public class CsvToDatasetChampionToDataframe {
	
	public void start() {
		
		SparkSession spark = SparkSession.builder()
		        .appName("CSV to Dataframe to Dataset<Champion>")
		        .master("local")
		        .getOrCreate();
		
		
		String filename = "src/main/resources/champions.csv";
		 
	    Dataset<Row> df = spark.read().format("csv")
	        .option("inferSchema", "true")
	        .option("multiline", true)
	        .option("header", true)
	        .option("sep", ",")
	        .load(filename);
	    
//	    System.out.println("Champion ingested in a dataframe: ");
//	    df.show(5);
//	    df.printSchema();
	    
	    Dataset<Champion> championDS = df.map(new ChampionMapper(), Encoders.bean(Champion.class));
	    
//	    System.out.println("Champion ingested in a dataset: ");
//	    championDS.show(5);
//	    championDS.printSchema();
	    
	    Dataset<Row> df2 = championDS.toDF();
	    df2 = df2.drop("date");
	    df2.show(10);
	    
	}
	
}

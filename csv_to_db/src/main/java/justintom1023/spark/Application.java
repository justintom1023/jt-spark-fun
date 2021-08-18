package justintom1023.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Properties;

public class Application {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", ""); // second argument needs to be filled
		
		SparkSession spark = new SparkSession.Builder()
				.appName("CSV to DB")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> df = spark.read().format("csv")
			.option("header", true)
			.load("src/main/resources/List_of_IWGP_Heavyweight_Champions.csv");
		
		df = df.withColumn("sentence", concat(df.col("Champion"),
				lit(" won the title at "), df.col("Event")))
				.filter(df.col("Champion").rlike("Okada"))
				.orderBy(df.col("Event").asc());
		
		String dbConnectionUrl = "jdbc:postgresql://localhost/my_data";
		Properties prop = new Properties();
		prop.setProperty("driver", "org.postgresql.Driver");
		prop.setProperty("user", "postgres");
		prop.setProperty("password", ""); // second argument needs to be filled
		
		df.write()
			.mode(SaveMode.Overwrite)
			.jdbc(dbConnectionUrl, "csv_to_db", prop);
		
	}
	
}

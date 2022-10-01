package justintom1023.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JSONLinesParser {

	public void parseJsonLines() {

		SparkSession spark = SparkSession.builder()
				.appName("JSON Lines to Dataframe")
				.master("local")
				.getOrCreate();
		
		StructType schema = DataTypes.createStructType(new StructField[] {
				
				DataTypes.createStructField("damage", DataTypes.IntegerType, false),
				DataTypes.createStructField("description", DataTypes.StringType, false),
				DataTypes.createStructField("name", DataTypes.StringType, false)
				
		});
		
		Dataset<Row> df = spark.read().format("json")
				.option("multiline", true)
				.schema(schema)
				.load("src/main/resources/pmtok_boots.json");
				
		df.show();
		df.printSchema();
		
	}

}

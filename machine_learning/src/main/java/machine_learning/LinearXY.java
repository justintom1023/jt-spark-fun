package machine_learning;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LinearXY {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkSession spark = new SparkSession.Builder()
				.appName("Linear Regression")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> xyDf = spark.read()
			.option("header", "true")
			.option("inferSchema", "true")
			.format("csv")
			.load("src/main/resources/train.csv");
		
		Dataset<Row> mldf = xyDf.withColumnRenamed("y", "label")
			.select("label", "x");
		
		String[] featureColumns = {"x"};
		
		VectorAssembler assembler = new VectorAssembler()
			.setInputCols(featureColumns)
			.setOutputCol("features");
		
		Dataset<Row> lblFeaturesDf = assembler.transform(mldf).select("label", "features");
		lblFeaturesDf = lblFeaturesDf.na().drop();
		
		LinearRegression lr = new LinearRegression();
		LinearRegressionModel model = lr.fit(lblFeaturesDf);
		model.summary().predictions().show();
		System.out.println("R Squared: " + model.summary().r2());
		
	}
	
}

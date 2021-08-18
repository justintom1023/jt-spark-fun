package machine_learning;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KMeansMallCustomers {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkSession spark = new SparkSession.Builder()
				.appName("K-Means Clustering")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> customersDf = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.format("csv")
				.load("src/main/resources/mall_customers.csv");
		
		Dataset<Row> featuresDf = customersDf.select("Genre", "Age", "Annual Income (k$)");
		
		String[] featureColumns = {"Genre", "Age", "Annual Income (k$)"};
		
		VectorAssembler assembler = new VectorAssembler();
		assembler.setInputCols(featureColumns).setOutputCol("features");
		
		Dataset<Row> trainingData = assembler.transform(featuresDf).select("features");
		
		KMeans kmeans = new KMeans().setK(100);
		
		KMeansModel model = kmeans.fit(trainingData);
		
		model.computeCost(trainingData);
		
		System.out.println(model.computeCost(trainingData));
		model.summary().predictions().show();
		
	}
	
}

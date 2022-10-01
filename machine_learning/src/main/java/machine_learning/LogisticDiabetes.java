package machine_learning;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LogisticDiabetes {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkSession spark = new SparkSession.Builder()
				.appName("Logistic Regression")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> diabetesDf = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.format("csv")
				.load("src/main/resources/diabetes2.csv");
		
		Dataset<Row> lblFeaturesDf = diabetesDf.withColumnRenamed("Outcome", "label")
			.select("label", "Pregnancies", "Glucose", "BloodPressure", "SkinThickness", "Insulin",
					"BMI", "DiabetesPedigreeFunction", "Age");
		
		lblFeaturesDf.na().drop();
		
		String[] featureColumns = {"Pregnancies", "Glucose", "BloodPressure", "SkinThickness",
				"Insulin", "BMI", "DiabetesPedigreeFunction", "Age"};
		
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(featureColumns)
				.setOutputCol("features");
		
		Dataset<Row>[] splitData = lblFeaturesDf.randomSplit(new double[] {.7, .3});
		Dataset<Row> trainingDf = splitData[0];
		Dataset<Row> testingDf = splitData[1];
		
		LogisticRegression logReg = new LogisticRegression();
		
		Pipeline pl = new Pipeline();
		pl.setStages(new PipelineStage[] {assembler, logReg});
		
		PipelineModel model = pl.fit(trainingDf);
		Dataset<Row> results = model.transform(testingDf);
		
		results.show(100);
		
	}
	
}

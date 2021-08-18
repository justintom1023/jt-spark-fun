package justintom1023.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class ArrayToDataset {

	public void start() {
		
		SparkSession spark = new SparkSession.Builder()
				.appName("Array to Dataset<String>")
				.master("local")
				.getOrCreate();
		
		String[] stringList = new String[] {"banana", "monkey", "cat", "banana", "duck", "monkey"};
		
		List<String> data = Arrays.asList(stringList);
		
		Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
		
		ds = ds.map(new StringMapper(), Encoders.STRING());
		ds.show(10);
		
		String stringValue = ds.reduce(new StringReducer());
		
		System.out.println(stringValue);
		
	}
	
	static class StringMapper implements MapFunction<String, String>, Serializable {

		private static final long serialVersionUID = 1L;

		public String call(String value) throws Exception {

			return "word: " + value;
			
		}
		
	}
	
	static class StringReducer implements ReduceFunction<String>, Serializable {

		private static final long serialVersionUID = 1L;

		public String call(String v1, String v2) throws Exception {

			return v1 + v2;
			
		}
		
	}
	
}

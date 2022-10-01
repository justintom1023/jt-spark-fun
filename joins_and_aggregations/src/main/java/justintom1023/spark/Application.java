package justintom1023.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Application {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
				.appName("Joins and Aggregations")
				.master("local")
				.getOrCreate();

		Dataset<Row> weaponsDf1 = spark.read().format("csv")
				.option("inferSchema", "true")
				.option("header", true)
				.load("src/main/resources/valorant_weapons.csv");

		Dataset<Row> weaponsDf2 = spark.read().format("csv")
				.option("inferSchema", "true")
				.option("header", true)
				.load("src/main/resources/valorant_weapons_2.csv");

		Dataset<Row> joinedData = weaponsDf1.join(weaponsDf2,
				weaponsDf1.col("name").equalTo(weaponsDf2.col("name")))
                .drop(weaponsDf2.col("name"))
                .drop("LDLR")
                .drop("BDLR")
                .drop("dispersion_bullet")
                .drop("dispersion_bullet_Scope");

        joinedData = joinedData.groupBy("weapon_type").agg(
            count("weapon_type").as("count_weapon_type"),
            max("bullets_on_charging").as("max_bullets_on_charging"),
            sum("price").as("sum_price"));
		
		joinedData.show();
		
	}

}

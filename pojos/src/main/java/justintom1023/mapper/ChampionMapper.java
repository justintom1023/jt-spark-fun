package justintom1023.mapper;

import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import justintom1023.pojos.Champion;

public class ChampionMapper implements MapFunction<Row, Champion> {

	private static final long serialVersionUID = 1L;

	@Override
	public Champion call(Row value) throws Exception {

		Champion c = new Champion();
		c.setNumber(value.getAs("No."));
		c.setChampion(value.getAs("Champion"));
		c.setEvent(value.getAs("Event"));
		c.setLocation(value.getAs("Location"));
		c.setReign(value.getAs("Reign"));
		c.setDays(value.getAs("Days"));
		c.setDefenses(value.getAs("Defenses"));
		
		String dateString = value.getAs("Date").toString();
		
		if (dateString != null) {
			
			SimpleDateFormat parser = new SimpleDateFormat("MMMM dd, yyyy");
			c.setDate(parser.parse(dateString));
			
		}
		
		return c;
		
	}
	
}

package logic.query1;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class PreProcessing {
	
	public static StructType getSchema(){
		StructField[] structFields = new StructField[]{
                		new StructField("area", DataTypes.StringType, true, Metadata.empty()),
		                new StructField("data", DataTypes.DateType, true, Metadata.empty()),
		                new StructField("totale", DataTypes.IntegerType, true, Metadata.empty()),
		                new StructField("numeroCentri", DataTypes.IntegerType, true, Metadata.empty())
		              
		        };
		        return new StructType(structFields);
		}
}

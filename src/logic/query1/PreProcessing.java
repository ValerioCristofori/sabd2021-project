package logic.query1;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class PreProcessing {
	
	public StructType getSchema(){
		StructField[] structFields = new StructField[]{
		                new StructField("data", DataTypes.DateType, true, Metadata.empty()),
		                new StructField("area", DataTypes.StringType, true, Metadata.empty()),
		                new StructField("totaleSomministrazioni", DataTypes.IntegerType, true, Metadata.empty()),
		                new StructField("numero", DataTypes.IntegerType, true, Metadata.empty())
		              
		        };
		        return new StructType(structFields);
		}
}

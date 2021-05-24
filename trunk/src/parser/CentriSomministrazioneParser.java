package parser;


import java.util.Arrays;
import java.util.List;

public class CentriSomministrazioneParser implements CSVParser{
	
	
	public static List<String> getArea( String csvLine ) {
		String area;
		
		String[] tok = csvLine.split(",");
		area = tok[0];
		return Arrays.asList(area);
	}
	

}

package parser;


import java.util.Arrays;
import java.util.List;

import entity.CentriSomministrazione;

public class CentriSomministrazioneParser implements CSVParser{
	
	public static CentriSomministrazione parseLine( String csvLine ) {
		String area;
		String denominazioneStruttura;
		
		String[] tok = csvLine.split(",");
		area = tok[0];
		denominazioneStruttura = tok[1];
		return new CentriSomministrazione( area, denominazioneStruttura );
	}
	
	public static List<String> getArea( String csvLine ) {
		String area;
		
		String[] tok = csvLine.split(",");
		area = tok[0];
		return Arrays.asList(area);
	}
}

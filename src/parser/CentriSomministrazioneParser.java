package parser;


import java.util.Arrays;
import java.util.List;

import entity.CentriSomministrazione;

public class CentriSomministrazioneParser implements CSVParser{
	
	
	public static List<String> getArea( String csvLine ) {
		String area;
		
		String[] tok = csvLine.split(",");
		area = tok[0];
		return Arrays.asList(area);
	}
	
	public static CentriSomministrazione parseCSV(String csvLine) {

		CentriSomministrazione centro = null;
        String[] csvValues = csvLine.split(",");

        if (csvValues.length != 7)
            return null;


        centro = new CentriSomministrazione(
                csvValues[0], // area
                csvValues[1] // denominazione struttura
                
        );

        return centro;
    }
}

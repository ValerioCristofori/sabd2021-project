package entity;

import java.io.Serializable;
import java.time.Month;

public class Somministrazione implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String data; //data che specifica il giorno delle somministrazioni
	private String area; //regione di somministrazione
	private String totale; //totale delle vaccinazioni nel giorno
	
	
	public String getArea() {
		return area;
	}
	public void setArea(String area) {
		this.area = area;
	}
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	public String getTotale() {
		return this.totale;
	}
	public void setTotale(String totale) {
		this.totale = totale;
	}
	public String getMese() {
		return this.data.split("-")[1];
	}

	@Override
	public String toString() {
		return String.format("Area %s%nData %s%nTotale somministrazioni %s%nNumero centri %d%n%n", this.area, this.data, this.totale);
	}
	
	
}

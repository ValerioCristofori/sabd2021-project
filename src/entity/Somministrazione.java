package entity;

import java.io.Serializable;
import java.util.Date;

public class Somministrazione implements Serializable{

	
	private String area; //regione di somministrazione
	private String data; //data che specifica il giorno delle somministrazioni
	private String totale; //totale delle vaccinazioni nel giorno
	private int numeroCentri; //numero di centri in quella regione
	
	
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
	public int getNumeroCentri() {
		return numeroCentri;
	}
	public void setNumeroCentri(int numeroCentri) {
		this.numeroCentri = numeroCentri;
	}
	
}

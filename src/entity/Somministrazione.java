package entity;

import java.io.Serializable;
import java.util.Date;

public class Somministrazione implements Serializable{

	
	private String area; //regione di somministrazione
	private String data; //data che specifica il giorno delle somministrazioni
	private int totale; //totale delle vaccinazioni nel giorno
	private int numeroCentri;
	
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
	public int getTotale() {
		return totale;
	}
	public void setTotale(int totaleSomministrazioni) {
		this.totale = totaleSomministrazioni;
	}
	public int getNumber() {
		return numeroCentri;
	}
	public void setNumber(int numeroCentri) {
		this.numeroCentri = numeroCentri;
	}
	
}

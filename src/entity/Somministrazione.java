package entity;

import java.util.Date;

public class Somministrazione {
	private Date data; //data che specifica il giorno delle somministrazioni
	private String area; //regione di somministrazione
	private Integer number; //totale delle vaccinazioni nel giorno
	public Date getData() {
		return data;
	}
	public void setData(Date data) {
		this.data = data;
	}
	public String getArea() {
		return area;
	}
	public void setArea(String area) {
		this.area = area;
	}
	public Integer getNumber() {
		return number;
	}
	public void setNumber(Integer number) {
		this.number = number;
	}
}

package entity;

public class CentriSomministrazione {
	private String area;
	private String denominazioneStruttura;
	private Integer numCentri;
	
	public CentriSomministrazione(String area, String denominazioneStruttura) {
		this.area = area;
		this.denominazioneStruttura = denominazioneStruttura;
	}
	public String getArea() {
		return area;
	}
	public void setArea(String area) {
		this.area = area;
	}
	public Integer getNumCentri() {
		return numCentri;
	}
	public void setNumCentri(Integer numCentri) {
		this.numCentri = numCentri;
	}
	public String getDenominazioneStruttura() {
		return denominazioneStruttura;
	}
	public void setDenominazioneStruttura(String denominazioneStruttura) {
		this.denominazioneStruttura = denominazioneStruttura;
	}
	
}

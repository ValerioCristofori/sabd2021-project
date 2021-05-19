package entity;

public class SommDonne {
    private String mese_giorno; //01-01
    private String area;
    private String fascia;
    private String totale;

    public String getMese_giorno() {
        return mese_giorno;
    }

    public void setMese_giorno(String mese_giorno) {
        this.mese_giorno = mese_giorno;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getFascia() {
        return fascia;
    }

    public void setFascia(String fascia) {
        this.fascia = fascia;
    }

    public String getTotale() {
        return totale;
    }

    public void setTotale(String totale) {
        this.totale = totale;
    }

    public String getMese(){
        return this.mese_giorno.split("-")[0];
    }
    public String getGiorno(){
        return this.mese_giorno.split("-")[1];
    }
}

package entity;

import java.util.Date;

public class SommDonne {
    private String data;    // data che specifica il giorno delle somministrazioni
    private String area;    // regione di somministrazione
    private String fascia;  // fascia anagrafica di riferimento
    private String totale;  // donne vaccinate in una specifica regione in uno specifico giorno (per una fascia anagrafica, per un fornitore)

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
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
}

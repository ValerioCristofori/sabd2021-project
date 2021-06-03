package utility;

import java.time.Duration;
import java.time.Instant;

public class TimeHandler {
    private Instant start;  // sincronizza un clock per il time handler
    private Instant stop;   // ferma il clock
    private long duration;  // tempo passato da start a stop - poi passato in millisecondi

    public void start() {
        this.start = Instant.now();
    }

    public long getDuration() {
        this.stop = Instant.now();
        this.duration = Duration.between(this.start,this.stop).toMillis();
        return duration;
    }

}

package utility;

import java.time.Duration;
import java.time.Instant;

public class TimeHandler {
    private Instant start;
    private Instant stop;
    private long duration;

    public void start() {
        this.start = Instant.now();
    }

    public long getDuration() {
        this.stop = Instant.now();
        this.duration = Duration.between(this.start,this.stop).toMillis();
        return duration;
    }

}

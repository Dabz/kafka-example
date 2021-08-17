package io.confluent.dabz;

import java.util.Date;

/**
 * Class exposed only for testing purposes
 */
public class TimestampProvider {
    public long getTimestamp() {
        return new Date().getTime();
    }
}

package io.confluent.dabz;

public class MockTimestampProvider extends TimestampProvider {
    public static long offset = 0;
    @Override
    public long getTimestamp() {
        return super.getTimestamp() + offset;
    }
}

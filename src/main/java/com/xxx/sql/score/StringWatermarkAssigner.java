package com.xxx.sql.score;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author 0x822a5b87
 */
public class StringWatermarkAssigner implements AssignerWithPeriodicWatermarks<String> {

    private long maxTimestamp = 0;

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTimestamp);
    }

    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        maxTimestamp = Math.max(maxTimestamp, recordTimestamp);
        return System.currentTimeMillis();
    }
}

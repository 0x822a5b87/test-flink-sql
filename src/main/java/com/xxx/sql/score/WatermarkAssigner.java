package com.xxx.sql.score;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author 0x822a5b87
 */
public class WatermarkAssigner implements AssignerWithPeriodicWatermarks<PlayerData> {

    private long maxTimestamp = 0;

    private long maxOutOfOrderness = 1000L;

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(this.maxTimestamp - this.maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(PlayerData element, long recordTimestamp) {
        maxTimestamp = Math.max(maxTimestamp, element.getTs());
        return element.getTs();
    }
}

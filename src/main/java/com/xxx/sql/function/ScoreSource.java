package com.xxx.sql.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * @author 0x822a5b87
 */
public class ScoreSource implements SourceFunction<String> {

    private static final int numCountries = 3;

    private boolean canceled = false;
    private long    delay    = 1;

    private final Random random;

    private final String[] scores;


    public ScoreSource(long averageInterArrivalTime) {
        delay  = averageInterArrivalTime;
        random = new Random();


        List<String> scoreList = new ArrayList<>();
        try {
            scoreList = Files.readAllLines(Paths.get("src/main/resources/score.csv"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        scores = new String[scoreList.size()];
        List<String> finalScoreList = scoreList;
        IntStream.range(0, scores.length).forEach(idx -> scores[idx] = finalScoreList.get(idx));
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        int currentCountry = 0;
        while (!canceled) {
            long ts = System.currentTimeMillis();
            sourceContext.collectWithTimestamp(randomScore(), ts);
            currentCountry = (currentCountry + 1) % numCountries;
            long sleepTime = (long) (random.nextGaussian() * 0.5D + delay);
            Thread.sleep(sleepTime);
        }
    }

    @Override
    public void cancel() {
        canceled = true;

    }

    private String randomScore() {
        int idx = random.nextInt();
        return scores[idx % scores.length];
    }
}

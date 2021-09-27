package com.xxx.sql.score;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 0x822a5b87
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class PlayerData {
    /**
     * 赛季，球员，出场，首发，时间，助攻，抢断，盖帽，得分
     */
    private String  season;
    private String  player;
    private String  playNum;
    private Integer firstCourt;
    private Double  time;
    private Double  assists;
    private Double  steals;
    private Double  blocks;
    private Double  scores;
    private long    ts;

    public PlayerData(String str) {
        String[] split = str.split(",");
        setSeason(String.valueOf(split[0]));
        setPlayer(String.valueOf(split[1]));
        setPlayNum(String.valueOf(split[2]));
        setFirstCourt(Integer.valueOf(split[3]));
        setTime(Double.valueOf(split[4]));
        setAssists(Double.valueOf(split[5]));
        setSteals(Double.valueOf(split[6]));
        setBlocks(Double.valueOf(split[7]));
        setScores(Double.valueOf(split[8]));
        setTs(System.currentTimeMillis());
    }
}
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
}
package com.xxx.sql.score;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @author 0x822a5b87
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Result {
    private Timestamp hopStart;
    private Timestamp hopEnd;
    private String    player;
    private long      num;
    private double    total;
}

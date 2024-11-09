package com.genesys.spark.tools;

import org.apache.spark.sql.api.java.UDF1;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

public class Timestamp2bucket implements UDF1<Timestamp, String> {
    @Override
    public String call(Timestamp ts) throws Exception {
        String bucket = null;
        if (ts == null)
            return bucket;
        try {
            bucket = ts.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyyMMddhh"));
        } catch (Exception e) {
            bucket = null;
        }
        return bucket;
    }


}

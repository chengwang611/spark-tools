package com.genesys.spark.tools;

import org.junit.Test;

import java.io.IOException;

public class BatchAggregatorTest {



    @Test
    public void run() throws IOException {
        String path=BatchAggregator.class.getClassLoader().getResource("data/input/genesys/genesys-input.csv").getFile();
        String outputpath=path.replace("data/input/genesys/genesys-input.csv","data/output/")+String.format("genesys/batch-agg-%d.csv",System.currentTimeMillis() );
        String[] args= {path,outputpath};
        BatchAggregator.main(args);
    }
}
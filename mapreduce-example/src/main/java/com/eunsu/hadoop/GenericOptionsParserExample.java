package com.eunsu.hadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class GenericOptionsParserExample {
    public static void main(String[] args) throws IOException{
        System.out.println(Arrays.toString(args));
        

        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String value1 = conf.get("mapreduce.map.memory.mb");
        Boolean value2 = conf.getBoolean("job.text", false);
        System.out.println("value1:" + value1 + "& value2: "+ value2);

        String[] remainingArgs = optionsParser.getRemainingArgs();
        System.out.println(Arrays.toString(remainingArgs));

    }
}

package com.eunsu.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.imageio.IIOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class MovieAverageTopKTest {
    @Test
    public void movieMapTest() throws IOException{
        MapDriver mapDriver = new MapDriver<Object, Text, Text, Text>()
            .withMapper(new MovieAverageRateTopK.MovieMapper())
            .withInput(new LongWritable(0), new Text("movieId,title,genres"))
            .withInput(new LongWritable(1), new Text("1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy"))
            .withInput(new LongWritable(2), new Text("2,Jumanji (1995),Adventure|Children|Fantasy"));
            


        // List<Pair<Text, Text>> result = mapDriver.run();
        //     // run vs runtest
        //     // runtest는 아웃풋 검증
        //     // run은 실제 결과가 어떻게 나오는지 확인할 수 있음
        // System.out.println(result);


        mapDriver.withOutput(new Text("1"), new Text("MToy Story (1995)"))
        .withOutput(new Text("2"), new Text("MJumanji (1995)"))
        .runTest();
    }


    @Test
    public void ratingMapTest() throws IOException{
        new MapDriver<Object, Text, Text, Text>()
            .withMapper(new MovieAverageRateTopK.RatingMapper())
            .withInput(new LongWritable(0), new Text("userId,movieId,rating,timestamp"))
            .withInput(new LongWritable(1), new Text("1,1,4.0,964982703"))
            .withInput(new LongWritable(2), new Text("5,1,4.0,847434962"))
            .withInput(new LongWritable(3), new Text("6,2,4.0,845553522"))
            .withInput(new LongWritable(4), new Text("8,2,4.0,839463806"))
            .withOutput(new Text("1"), new Text("R4.0"))
            .withOutput(new Text("1"), new Text("R4.0"))
            .withOutput(new Text("2"), new Text("R4.0"))
            .withOutput(new Text("2"), new Text("R4.0"))
            .runTest();
    }


    @Test
    public void movieRatingJoinReduceTest() throws IOException{
        new ReduceDriver<Text, Text, Text, Text>()
            .withReducer(new MovieAverageRateTopK.MovieRatingJoinReducer())
            .withInput(new Text("1"), Arrays.asList(new Text("MToy Story (1995)"), new Text("R4.0"), new Text("R4.0")))
            .withInput(new Text("2"), Arrays.asList(new Text("MJumanji (1995)"), new Text("R4.0"), new Text("R4.0")))
            .withOutput(new Text("Toy Story (1995)"), new Text("4.0"))
            .withOutput(new Text("Jumanji (1995)"), new Text("4.0"))
            .runTest();
            
    }

    
  
}

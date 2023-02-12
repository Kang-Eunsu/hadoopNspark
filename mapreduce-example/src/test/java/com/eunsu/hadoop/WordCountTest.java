package com.eunsu.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {
    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    // test 어노테이션 진행하기 전 항상 실행
    public void setUp(){
        mapDriver = new MapDriver<>(new WordCount.TokenizerMapper()); // mapDrvier 초기화
        // 생성자로 mapper를 전달
        reduceDriver = new ReduceDriver<>(new WordCount.IntSumReducer()); //reduceDriver 초기화
        mapReduceDriver = new MapReduceDriver<>(new WordCount.TokenizerMapper(), new WordCount.IntSumReducer());
        // 2개의 인자가 필요함
        // mapReduceDriver이기 때문에 첫번째는 Mapper, 두번째는 Reducer가 필요함
    }

    @Test
    // Mapper 테스트
    // runtest를 하면 바로 입력값과 출력값을 넣어주어 결과를 바로 확인하게 해줌
    public void wordCountMapTest() throws IOException{
        new MapDriver<Object, Text, Text, IntWritable>()
            .withMapper(new WordCount.TokenizerMapper())
            .withInput(new LongWritable(0L), new Text("dog dog cat cat owl cat"))
            .withOutput(new Text("dog"), new IntWritable(1))
            .withOutput(new Text("dog"), new IntWritable(1))
            .withOutput(new Text("cat"), new IntWritable(1))
            .withOutput(new Text("cat"), new IntWritable(1))
            .withOutput(new Text("owl"), new IntWritable(1))
            .withOutput(new Text("cat"), new IntWritable(1))
            .runTest();
    }

    @Test 
    // Reducer 테스트
    public void wordCountReduceTest() throws IOException{
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
            .withReducer(new WordCount.IntSumReducer())
            // 키 값으로 정렬되어 들어가기 때문에 cat -> dog -> owl 순 (알파벳 기준)
            .withInput(new Text("cat"), Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(1)))
            .withInput(new Text("dog"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
            .withInput(new Text("owl"), Arrays.asList(new IntWritable(1)))
            .withOutput(new Text("cat"), new IntWritable(3))
            .withOutput(new Text("dog"), new IntWritable(2))
            .withOutput(new Text("owl"), new IntWritable(1))
            .runTest();
    }

    @Test
    public void wordCountMapTest2() throws IOException {
        List<Pair<Text, IntWritable>> result = mapDriver
            .withInput(new LongWritable(0L), new Text("dog dog cat cat owl cat"))
            .run();
        System.out.println(result); 
        // 결과값 : [(dog, 1), (dog, 1), (cat, 1), (cat, 1), (owl, 1), (cat, 1)]
    }

    @Test
    public void wordCountReduceTest2() throws IOException{
        List<Pair<Text, IntWritable>> result = reduceDriver
            .withInput(new Text("cat"), Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(1)))
            .withInput(new Text("dog"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
            .withInput(new Text("owl"), Arrays.asList(new IntWritable(1)))
            .run();

        System.out.println(result);
        // result : [(cat, 3), (dog, 2), (owl, 1)]
    }

    @Test
    public void wordCountTest() throws IOException {
        mapReduceDriver
            .withInput(new LongWritable(0L), new Text("dog dog cat cat owl cat"))
            .withOutput(new Text("cat"), new IntWritable(3))
            .withOutput(new Text("dog"), new IntWritable(2))
            .withOutput(new Text("owl"), new IntWritable(1))
            .runTest();

    }

    @Test 
    public void wordCountTest2() throws IOException{
        List<Pair<Text, IntWritable>> result = mapReduceDriver
            .withInput(new LongWritable(0L), new Text("dog dog cat cat owl cat"))
            .run();

        System.out.println(result);
        // result : [(cat, 3), (dog, 2), (owl, 1)]
    }

    //여기까지 WordCount.java를 이용한 테스트

    //여기부터 WordCountWithCounter.java를 이용한 테스트
    @Test
    public void WordCountWithCounter() throws IOException{
        MapDriver<Object, Text, Text, IntWritable> mapDriver1 = new MapDriver<>(new WordCountWithCounter.TokenizerMapper());
        mapDriver1
            .withInput(new LongWritable(0L), new Text("dog dog cat cat owl cat *dog"))
            .run();

        System.out.println(mapDriver1.getCounters().findCounter(WordCountWithCounter.Word.WITHOUT_SPECIAL_CHARACTER).getValue());
        // result : 6
        assertEquals(mapDriver1.getCounters().findCounter(WordCountWithCounter.Word.WITHOUT_SPECIAL_CHARACTER).getValue(), 3);
        System.out.println(mapDriver1.getCounters().findCounter(WordCountWithCounter.Word.WITH_SPECIAL_CHARACTER).getValue());
        // result : 0
    }




    




}

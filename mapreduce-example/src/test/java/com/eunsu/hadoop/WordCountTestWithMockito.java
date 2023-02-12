package com.eunsu.hadoop;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.junit.Test;
import org.mockito.InOrder;

import com.eunsu.hadoop.WordCount.TokenizerMapper;
import com.eunsu.hadoop.WordCountWithCounter.Word;

public class WordCountTestWithMockito {
    @Test 
    public void wordCountMapTest() throws IOException, InterruptedException{
        TokenizerMapper mapper = new TokenizerMapper();
        Mapper.Context context = mock(Mapper.Context.class);
        mapper.word = mock(Text.class);
        mapper.map(new LongWritable(0), new Text("dog dog cat"), context);

        InOrder inOrder = inOrder(mapper.word, context);
        inOrder.verify(mapper.word).set(eq("dog"));
        inOrder.verify(context).write(eq(mapper.word), eq(new IntWritable(1)));
        // key , value

        inOrder.verify(mapper.word).set(eq("dog"));
        inOrder.verify(context).write(eq(mapper.word), eq(new IntWritable(1)));
        // key , value

        inOrder.verify(mapper.word).set(eq("cat"));
        inOrder.verify(context).write(eq(mapper.word), eq(new IntWritable(1)));
        // key , value
    }


    @Test
    public void wordCountReduceTest() throws IOException, InterruptedException{
        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
        Reducer.Context context = mock(Reducer.Context.class);
        
        List<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(1));

        reducer.reduce(new Text("dog"), values, context);
        verify(context).write(new Text("dog"), new IntWritable(2)); 
        // key 값 몇 번 나왔는지 세기
        
    }

    @Test
    public void counterTest() throws IOException, InterruptedException{
        WordCountWithCounter.TokenizerMapper mapper = new WordCountWithCounter.TokenizerMapper();
        Mapper.Context context = mock(Mapper.Context.class);
        Counter counter = mock(Counter.class);
        when(context.getCounter(Word.WITHOUT_SPECIAL_CHARACTER)).thenReturn(counter);
        // context에서 getCounter라는 함수를 호출하게 되면 Counter를 리턴하도록 설정을 해두었음

        mapper.map(new LongWritable(0), new Text("dog dog cat"), context);
        verify(counter, times(3)).increment(1);
    }
}

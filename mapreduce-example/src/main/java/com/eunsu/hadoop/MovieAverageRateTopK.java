package com.eunsu.hadoop;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieAverageRateTopK extends Configured implements Tool{

    private final static int K = 30; // 상위 30개

    public static class MovieMapper extends Mapper<Object, Text, Text, Text>{
        private Text movieId = new Text(); // key 출력 Text
        private Text outValue = new Text(); // value 출력 Text

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] columns = value.toString().split(",");
            if (columns[0].equals("movieId")){
                return; // 첫번째줄은 헤더 정보를 가지고 있기 때문에 그냥 넘어가면 됨
            }
            movieId.set(columns[0]);
            outValue.set("M" + columns[1]); //movies.csv에서 온 걸 알아야 하기 때문에 M 키워드 추가
            context.write(movieId, outValue);
        }

    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text>{
        private Text movieId = new Text(); // key 출력 Text
        private Text outValue = new Text(); // value 출력 Text

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] columns = value.toString().split(",");
            if (columns[1].equals("movieId")){
                return; // 첫번째줄은 헤더 정보를 가지고 있기 때문에 그냥 넘어가면 됨
            }
            movieId.set(columns[1]);
            outValue.set("R" + columns[2]); //movies.csv에서 온 걸 알아야 하기 때문에 M 키워드 추가
            context.write(movieId, outValue);
        }
    }

    public static class MovieRatingJoinReducer extends Reducer<Text, Text, Text, Text>{
        private List<String> ratingList = new ArrayList<>();
        private Text movieName = new Text();
        private Text outValue = new Text();


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for (Text value : values){
                if (value.charAt(0) == 'M'){ // MovieMapper로부터 온 데이터
                    movieName.set(value.toString().substring(1));
                } else if (value.charAt(0) == 'R'){ // RatingMapper로부터 온 데이터
                    ratingList.add(value.toString().substring(1));
                }
            }

            double average = ratingList.stream().mapToDouble(Double::parseDouble).average().orElse(0.0);
            outValue.set(String.valueOf(average));
            context.write(movieName, outValue);
        }
    }

    public static class TopKMapper extends Mapper<Object, Text, Text, Text>{
        // TopKMapper는 앞선 Map Reduce의 결과물을 받게 됨 (MovieRatingJoinReducer)

        // treemap을 이용해서 상위 30개의 가장 평점이 높은 매퍼들을 트리맵에 저장해놓을 거임
        // 트리맵에 저장해놓고 있다가 연쇄적으로 맵이 종료될 때 cleanuP 함수가 호출이 되는데 트리맵에 저장되어있는 상위 30개의 데이터를 맵의 출력으로
        // TreeMap은 정렬이 되어있는 Map인데, key값을 기준으로 정렬이 되어있음
        // 오름차순 기준으로 데이터가 들어감
        private TreeMap<Double, Text> topKMap = new TreeMap<>();
        

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] columns = value.toString().split("\t");
            // MovieRatingJoinReducer가 데이터를 내보낼때
            // context.write(movieName, outValue) 이 부분에서 movieName가 outValue가 탭 기준으로 합쳐져서 내보냄
            
            topKMap.put(Double.parseDouble(columns[1]), new Text(columns[0]));
            // 오름차순이기 때문에 데이터가 들어왔을 때 30개가 넘으면 가장 앞의 데이터를 지우는 형식으로 항상 30개만 남겨놓도록 함
            
            if (topKMap.size() > K){
                topKMap.remove(topKMap.firstKey()); // 가장 앞의 데이터 삭제
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
        // 해당 함수는 mapper가 종료될 때 딱 한 번 호출되는 함수임
            for (Double k : topKMap.keySet()) {
                context.write(new Text(k.toString()), topKMap.get(k));
            }

        }

    }

    public static class TopKReducer extends Reducer<Text, Text, Text, Text>{
        private TreeMap<Double, Text> topKMap = new TreeMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            // values에 영화제목이 들어있을 것이고, key에 평점이 들어있을 것임
            for (Text value : values){
                topKMap.put(Double.parseDouble(key.toString()), value);
                if (topKMap.size() > K){
                    topKMap.remove(topKMap.firstKey());
                }
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception{
        Job job = Job.getInstance(getConf(), "MovieAverageRateTopK first");
        job.setJarByClass(MovieAverageRateTopK.class);
        job.setReducerClass(MovieRatingJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        int returnCode = job.waitForCompletion(true) ? 0 : 1;
        if (returnCode == 0){
            Job job2 = Job.getInstance(getConf(), "MovieAverageRateTopK Second");
            job2.setJarByClass(MovieAverageRateTopK.class);
            job2.setMapperClass(TopKMapper.class);
            job2.setReducerClass(TopKReducer.class);
            job2.setNumReduceTasks(1);
            // 실행되는 리듀서의 갯수가 하나로 지정을 해서 리듀서 안에 모든 데이터가 모여서 최종적으로 상위 30개의 평점이 가장 높은 결과 출력
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // 두번째 맵리듀스의 인풋
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            return job2.waitForCompletion(true) ? 0 : 1;

        }


        return 1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new MovieAverageRateTopK(), args);
        System.exit(exitCode);
    }
  
}

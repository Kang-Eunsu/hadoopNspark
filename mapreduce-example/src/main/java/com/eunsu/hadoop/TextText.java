package com.eunsu.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

// 복합키 클래스 정의 | 첫번째 key 타입도 Text, 두번째 key 타입도 Text
public class TextText implements WritableComparable<TextText>{
    private Text first;
    private Text second;

    public TextText(){
        set(new Text(), new Text());
    }
    public TextText(String first, String second){
        set(new Text(first), new Text(second));
    }

    public TextText(Text first, Text second){
        set(first, second);
    }

    public void set(Text first, Text second){
        this.first = first;
        this.second = second;
    }

    public Text getFirst(){
        return first;
    }
    public Text getSecond(){
        return second;
    }

    @Override
    public int compareTo(TextText o){
        int cmp = first.compareTo(o.first);
        if (cmp != 0){
            return cmp;
        }

        return second.compareTo(o.second);
    } 

    @Override
    public void write(DataOutput out) throws IOException{
        first.write(out);
        second.write(out);
    }

    //직렬화된 데이터를 읽을 때
    @Override
    public void readFields(DataInput in) throws IOException{
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int hashCode(){
        return first.hashCode() * 163 + second.hashCode(); //소수를 주로 곱해줌 -> 여기선 163
    }

    @Override
    public boolean equals(Object obj){
        if (obj instanceof TextText){
            TextText tt = (TextText) obj;
            return first.equals(tt.first) && second.equals(tt.second);
        }

        return false;
    }

    @Override
    public String toString(){
        return first.toString() + "," + second.toString();  
    }


}

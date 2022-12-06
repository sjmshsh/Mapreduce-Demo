package com.lxy.mapreduce;

/**
 * @Author 写你的名字
 * @Date 2022/11/28 21:46 （可以根据需要修改）
 * @Version 1.0 （版本号）
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class phoneMapper extends Mapper<LongWritable, Text, Text, Text> {
    private int[] timeRangeList;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String timeRange = configuration.get("timeRange");
        if (timeRange == null) {
            throw new RuntimeException("not receive the arguments......");
        }
        if ("1".equals(timeRange)) {
            timeRangeList = new int[1];
            timeRangeList[0] = 24;
            return;
        }
        String[] timeRangeString = timeRange.split("-");
        timeRangeList = new int[timeRangeString.length];
        for (int i = 0; i < timeRangeString.length; i++) {
            timeRangeList[i] = Integer.parseInt(timeRangeString[i]);
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        String values[] = value.toString().split("\\s+");
        String userId = values[0];
        String timeString = values[4];
        String baseStation = values[2];
        int hour = Integer.parseInt(timeString.split(":")[0]);
        int startHour = 0;
        int endHour = 0;
        for (int i = 0; i < timeRangeList.length; i++) {
            if (hour < timeRangeList[i]) {
                if (i == 0) {
                    startHour = 0;
                }else {
                    startHour = timeRangeList[i - 1];
                }
                endHour = timeRangeList[i];
                break;
            }
        }
        if (startHour == 0 && endHour == 0) {
            return;
        }
        String timeRange = context.getConfiguration().get("timeRange");
        if ("1".equals(timeRange)) {
            context.write(new Text(userId + "\t"), new Text(baseStation + "-" +
                    timeString));
            return;
        }
        context.write(new Text(userId + "," + startHour + "-" + endHour + ","), new
                Text(baseStation + "-" + timeString));
    }
}
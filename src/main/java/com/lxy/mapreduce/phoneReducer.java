package com.lxy.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class phoneReducer extends Reducer<Text, Text, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        List<String> valueList = new LinkedList<String>();
        Map<String, Long> residenceTimeMap = new TreeMap<String, Long>();
        for (Text value : values) {
            String item = value.toString();
            valueList.add(item);
        }
        if (valueList == null || valueList.size() <= 1) {
            return;
        }
        Collections.sort(valueList, new Comparator<String>() {
            public int compare(String o1, String o2) {
                o1 = o1.split("-")[1];
                o2 = o2.split("-")[1];
                return o1.compareTo(o2);
            }
        });
        for (int i = 0; i < valueList.size() - 1; i++) {
            String station = valueList.get(i).split("-")[0];
            String time1 = valueList.get(i).split("-")[1];
            String time2 = valueList.get(i + 1).split("-")[1];
            DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
            Date date1 = null;
            Date date2 = null;
            try {
                date1 = dateFormat.parse(time1);
                date2 = dateFormat.parse(time2);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if (date1.after(date2)) {
                continue;
            }
            long minute = (date2.getTime() - date1.getTime());
            minute = minute / 1000 / 60;
            Long count = residenceTimeMap.get(station);
            if (count == null) {
                residenceTimeMap.put(station, minute);
            } else {
                residenceTimeMap.put(station, count + minute);
            }
        }
        valueList = null;
        List<Map.Entry<String, Long>> entryList = new ArrayList<Entry<String, Long>>
                (residenceTimeMap.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return -(o1.getValue().compareTo(o2.getValue()));
            }
        });
        Set<String> keySet = residenceTimeMap.keySet();
        String timeRange = context.getConfiguration().get("timeRange");
        if ("1".equals(timeRange)) {
            String mapKey = keySet.iterator().next();
            context.write(new Text(key + "\t" + mapKey + "\t"), new
                    LongWritable(residenceTimeMap.get(mapKey)));
            return;
        }
        for (String mapKey : keySet) {
            context.write(new Text(key + "\t" + mapKey + "\t"), new
                    LongWritable(residenceTimeMap.get(mapKey)));
        }
        residenceTimeMap = null;
    }
}
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Jaccard {
    public static class IntPair implements WritableComparable<IntPair> {

        private int first;
        private int second;
        public IntPair(){

        }
        public IntPair(int f, int s){
            if (f <= s){
                set(f, s);
            } else {
                set(s, f);
            }
        }

        public void set(int f, int s){
            this.first = f;
            this.second = s;
        }
        public int getFirst(){
            return first;
        }
        public int getSecond(){
            return second;
        }

        public String output(){
            String result = "(" + first + "," + second + ")";
            return result;
        }

        public int compareTo(IntPair target) {
            if (first == target.getFirst()){
                return second - target.getSecond();
            } else{
                return first - target.getFirst();
            }
        }

        public void readFields(DataInput arg0) throws IOException {
            // TODO Auto-generated method stub
            first = arg0.readInt();
            second = arg0.readInt();

        }

        public void write(DataOutput arg0) throws IOException {
            // TODO Auto-generated method stub
            arg0.writeInt(first);
            arg0.writeInt(second);

        }
    }


    public static class FirstMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
            double threshold = 0.3;
            String val = value.toString();
            String num = val.substring(0,val.indexOf(" "));
            String name = val.substring(val.indexOf(" ") + 1);
            ArrayList<String> words = new ArrayList<String>();
            for(int i = 0; i < name.length()-2;i++){
                String item = name.substring(i,i+3);
                words.add(item);
            }
            String kvPair = num + "|";
            for(int i = 0; i < words.size(); i++){
                kvPair += words.get(i) + "#";
            }
            kvPair = kvPair.substring(0,kvPair.length()-1);
//            double prefixLength = words.size() - Math.ceil(words.size() * threshold) + 1;
            int prefixLength = words.size();
            for(int i = 0; i < prefixLength; i++) {
                context.write(new Text(words.get(i)),new Text(kvPair));
            }
        }
    }

    public static class FirstPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text arg0, Text arg1, int numOfPartition) {
            return (arg0.hashCode() & Integer.MAX_VALUE) % numOfPartition;
        }

    }

    public static class FirstReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double threshold = 0.3;
            ArrayList<String> dataList = new ArrayList<String>();
            for(Text value : values){
                dataList.add(value.toString());
            }
            for(int i = 0; i < dataList.size(); i++) {
                for(int j = i+1; j < dataList.size(); j++){
                    double numOfCommon = 0.0;
                    double similarity = 0.0;

                    int id1 = Integer.parseInt(StringUtils.split(dataList.get(i), '|')[0]);
                    int id2 = Integer.parseInt(StringUtils.split(dataList.get(j), '|')[0]);

                    String id1Data = StringUtils.split(dataList.get(i), '|')[1];
                    String id2Data = StringUtils.split(dataList.get(j), '|')[1];
                    String id1DataList[] = StringUtils.split(id1Data, '#');
                    String id2DataList[] = StringUtils.split(id2Data, '#');

                    int id1DataLength = id1DataList.length;
                    for(int k = 0; k < id2DataList.length; k++){
                        if(ArrayUtils.contains(id1DataList, id2DataList[k])){
                            numOfCommon++;
                        }else{
                            id1DataLength++;
                        }
                    }

                    similarity = numOfCommon / id1DataLength;

                    if(similarity >= threshold){
                        context.write(new Text("" + id1), new Text(id2 + "\t" + similarity));
                    }

                }
            }
        }
    }

    public static class FinalMapper extends Mapper<Object, Text, IntPair, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            ArrayList<String> line = new ArrayList<String>();
            StringTokenizer token = new StringTokenizer(value.toString(), "\t");

            while (token.hasMoreTokens()) {
                line.add(token.nextToken());
            }
            context.write(new IntPair(Integer.parseInt(line.get(0)), Integer.parseInt(line.get(1))), new Text(line.get(2)));
        }
    }

    public static class FinalPartitioner extends Partitioner<IntPair, Text>{

        @Override
        public int getPartition(IntPair arg0, Text arg1, int numOfPartition) {
            return Math.abs((arg0.getFirst() + arg0.getSecond()) % numOfPartition);
        }

    }



    public static class FinalReducer extends Reducer<IntPair, Text, Text, Text> {

        public void reduce(IntPair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String sim = "";
            for (Text val : values) {
                sim = val.toString();
                break;
            }
            String outputKey = key.output();
            context.write(new Text(outputKey), new Text(sim));
        }
    }


    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        String input = args[0];
        String output = args[1];
//        String tmpTask = args[2];
//        int tasks = Integer.parseInt(tmpTask);
        Job job = Job.getInstance(conf, "First");
//        job.setNumReduceTasks(tasks);
        job.setJarByClass(Jaccard.class);
        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReducer.class);
//        job.setPartitionerClass(FirstPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        Path temp = new Path(output + "_temp");
        FileOutputFormat.setOutputPath(job, temp);
        job.waitForCompletion(true);

        job = Job.getInstance(conf, "Final");
//        job.setNumReduceTasks(tasks);
        job.setJarByClass(Jaccard.class);
        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
//        job.setPartitionerClass(FinalPartitioner.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, temp);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

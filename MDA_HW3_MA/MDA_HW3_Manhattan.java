package org.apache.hadoop.examples;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.*;
import java.util.*;
import java.net.*;


public class MDA_HW3_Manhattan {
    private static int knum = 10;
    private static int fnum = 58;

    public static class kmeansMapper
        extends Mapper<Object, Text, IntWritable, Text>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String [] parseText = value.toString().split(" ");
            int cluster=0;
            double min=Double.MAX_VALUE;
            for(int i=0;i<knum;i++)
            {
                double cost = 0;
                String v = conf.get(String.valueOf(i));
                String [] centroid = v.split(" ");
                for(int j=0;j<fnum;j++)
                {
                    cost+=Math.abs(Double.parseDouble(parseText[j])-Double.parseDouble(centroid[j]));
                }
                //cost = Math.sqrt(cost);
                if(cost < min){
                  cluster = i;
                  min = cost;
                }
            }

            context.write(new IntWritable(cluster),new Text(value.toString()));

        }
    }


    public static class kmeansReducer
        extends Reducer<IntWritable,Text,NullWritable,Text> {

    public void reduce(IntWritable key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
                  Configuration conf = context.getConfiguration();

                  double [] total = new double[fnum];
                  ArrayList<ArrayList<Double>> calCost = new ArrayList<ArrayList<Double>>();

                  for(Text val : values){
                      String [] parseText = val.toString().split(" ");
                      for(int i=0;i<fnum;i++){
                        total[i]+=Double.parseDouble(parseText[i]);
                      }
                      ArrayList<Double> sList = new ArrayList<Double>();
                      for(int i=0;i<fnum;i++){
                        sList.add(Double.parseDouble(parseText[i]));
                      }
                      calCost.add(sList);
                  }
                  for(int i=0;i<fnum;i++){
                    total[i] = total[i] / (double)calCost.size();
                  }
                  String v = conf.get(String.valueOf(key));
                  String [] centroid = v.split(" ");

                  double totalCost = 0;
                  for(int i=0;i<calCost.size();i++){
                    for(int j=0;j<fnum;j++){
                      totalCost+=Math.abs(calCost.get(i).get(j) - Double.parseDouble(centroid[j]));
                    }
                  }

                  String writeout = "";
                  for(int i=0;i<fnum;i++){
                    if(i==0)
                      writeout = writeout + String.valueOf(total[i]);
                    else
                      writeout = writeout + ' ' + String.valueOf(total[i]);
                  }
                  context.write(null,new Text(writeout+" "+String.valueOf(totalCost)));
          }
    }

private static void calDist(Map<String,String> path,int iter){
    try{
      double [][] center = new double[knum][fnum];
      FileSystem fs = FileSystem.get(new Configuration());
      FSDataOutputStream os = null;
      BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(path.get("outCenter")+String.valueOf(iter)+"/part-r-00000"))));
      String line;
      line=br.readLine();
      int index=0;
      while (line != null){
        String [] par = line.split(" ");
        for(int i=0;i<fnum;i++){
            center[index][i] = Double.parseDouble(par[i]);
        }
        line=br.readLine();
        index++;
      }

      String content="";
      for(int i=0;i<knum;i++){
        for(int j=i+1;j<knum;j++){
            double cost=0;
            for(int k=0;k<fnum;k++){
              cost+=Math.pow(center[i][k]-center[j][k],2);
            }
            cost=Math.sqrt(cost);
            content = content + String.valueOf(i+1)+" between "+String.valueOf(j+1)+" "+String.valueOf(cost)+"\n";
        }
      }
      byte[] buff = content.getBytes();
      os = fs.create(new Path("/user/root/output/dist"));
      os.write(buff, 0, buff.length);
      if(os != null)
      os.close();
      fs.close();
    }
    catch (Exception e){

    }

}

public static void main (String[] args) throws Exception {
    Map<String, String> path = new HashMap<String, String>();
    path.put("data","/user/root/data/data.txt");
    path.put("outCenter","/user/root/output/outCenter");
    path.put("outCost","/user/root/output/outCost");

    int iter=20;

    for(int i=0;i<iter;i++){
      Configuration conf = new Configuration();
      if(i==0)
        conf.set("c",path.get("outCenter")+String.valueOf(i));
      else
        conf.set("c",path.get("outCenter")+String.valueOf(i)+"/part-r-00000");
      FileSystem fs = FileSystem.get(conf);
      BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(conf.get("c")))));
      String line;
      line=br.readLine();
      int it=0;
      while (line != null){
          conf.set(String.valueOf(it),line);
          line=br.readLine();
          it++;
      }
      fs.close();

      Job job = new Job(conf, "Kmeans"+String.valueOf(i));
      job.setJarByClass(MDA_HW3_Manhattan.class);
      job.setMapperClass(kmeansMapper.class);
      job.setReducerClass(kmeansReducer.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(path.get("data")));
      FileOutputFormat.setOutputPath(job, new Path(path.get("outCenter")+String.valueOf(i+1)));
      job.waitForCompletion(true);


    }
    calDist(path,iter);

    try{

      double [] cost = new double[iter+1];
      FileSystem fs = FileSystem.get(new Configuration());
      FSDataOutputStream os = null;
      for(int i=1;i<=iter;i++){
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(path.get("outCenter")+String.valueOf(i)+"/part-r-00000"))));
        String line;
        line=br.readLine();
        while (line != null){
          String [] par = line.split(" ");
          cost[i] = cost[i] + Double.parseDouble(par[fnum]);
          line=br.readLine();
        }
      }
      String content="";
      for(int i=1;i<=iter;i++){
        content = content + String.valueOf(cost[i]) + "\n";
      }
      byte[] buff = content.getBytes();
      os = fs.create(new Path(path.get("outCost")));
      os.write(buff, 0, buff.length);
      if(os != null)
      os.close();
      fs.close();

    }
    catch (Exception e){

    }
}



}

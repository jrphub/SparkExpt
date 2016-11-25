package com.saama.spark;



import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount 
{
	public static void main(String[] args) 
	{
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
		JavaSparkContext jSparkCntxt = new JavaSparkContext(conf);
		
		jSparkCntxt.addFile("sample.txt");
		jSparkCntxt.setLogLevel("FATAL");
		
		JavaRDD<String> lines = jSparkCntxt.textFile("sample.txt");
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			
			public Iterable<String> call(String line) throws Exception 
			{
				Thread.sleep(1000);
				return Arrays.asList(line.split(" ")); 
			}
		});
		
		JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
			
			public Tuple2<String, Integer> call(String word) throws Exception 
			{
				Thread.sleep(1000);
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> finalWordCount =  wordCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer val1, Integer val2) throws Exception 
			{
				Thread.sleep(1000);
				return val1 + val2;
			}
		});
		
		//finalWordCount.saveAsTextFile("file:///home/adongare/ajit/workspace/Spark/output");
		Iterator<Tuple2<String, Integer>> iterator = finalWordCount.collect().iterator();
		
		while(iterator.hasNext())
			System.out.println(iterator.next());
		
		jSparkCntxt.close();
	}
}

package test;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.base.Joiner;

public class TestApp {
	
	
	public static int distance(String a, String b) {
        a = a.toLowerCase();
        b = b.toLowerCase();
        // i == 0
        int [] costs = new int [b.length() + 1];
        for (int j = 0; j < costs.length; j++)
            costs[j] = j;
        for (int i = 1; i <= a.length(); i++) {
            // j == 0; nw = lev(i - 1, j)
            costs[0] = i;
            int nw = i - 1;
            for (int j = 1; j <= b.length(); j++) {
                int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]), a.charAt(i - 1) == b.charAt(j - 1) ? nw : nw + 1);
                nw = costs[j];
                costs[j] = cj;
            }
        }
        return costs[b.length()];
    }
	
	
	public static void main(String[] args){
		String testFile = "textFile.txt";
		JavaSparkContext sc = new JavaSparkContext("local", "SimpleAPP");
		
		JavaRDD<String> logData = sc.textFile(testFile).cache();
		
		
		/**
		 * Print out the data
		for ( String x : logData.collect()){
			System.out.println(x);
		}
		**/
		
		JavaPairRDD<String, String> pairs = logData.mapToPair(new PairFunction<String, String, String>() {
			
			public Tuple2<String,String> call (String s){
				return new Tuple2<String,String>(s.split(":")[0],s.split(":")[1]);
			}
		}).cache();
		
		JavaPairRDD<String, Iterable<String>> combine = pairs.groupByKey().cache();
		
		for (Tuple2<String,Iterable<String>> x : combine.collect()){
			
			System.out.println(x);
		}
		
		
		JavaPairRDD<String, Iterable<String>> clusters = combine.ma
		
		sc.close();
		
		
	}

}

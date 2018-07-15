package spark_visualizer.mapreduce;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MapReduceAlgo {
	
	private JavaSparkContext jsc;
	private JavaPairRDD<String, String> fromRDD, toRDD;
	
	public MapReduceAlgo(int nExecutors) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		jsc = new JavaSparkContext("local[" + nExecutors + "]", "Spark Visualizer");
	}

	public JavaPairRDD<String, String> getFromRDD() { return fromRDD; }
	
	public JavaPairRDD<String, String> getToRDD() { return toRDD; }
	
	public void setFromRDD(JavaPairRDD<String, String> rdd) { fromRDD = rdd; }
	
	public void setToRDD(JavaPairRDD<String, String> rdd) { toRDD = rdd; }
	
	public JavaPairRDD<String, String> parallelize(ArrayList<Tuple2<String, String>> dataset) { 
		return jsc.parallelizePairs(dataset);
	}
}

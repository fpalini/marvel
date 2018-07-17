package spark_visualizer.orchestrator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;

import scala.Tuple2;
import spark_visualizer.mapreduce.MapReduceAlgo;

public class Orchestrator {
	private MapReduceAlgo mapReduceAlgo;
	private int nExecutors;
	
	public Orchestrator(int nExecutors) {
		this.nExecutors = nExecutors;
		mapReduceAlgo = new MapReduceAlgo(nExecutors); // setup of the Spark system
	}
	
	public void createRDDfromFile(int size, File input_file, String split_char, int key_column, int value_column) {
        ArrayList<Tuple2<String, String>> dataset = new ArrayList<>();
        BufferedReader reader = null;
        
        try {
	        reader = new BufferedReader(new FileReader(input_file));
	
	        String key = null;
	        String value = null;
	        
	        int el_counter = 0;
	        String line, key_value_line[];
        
	        while((line = reader.readLine()) != null && el_counter++ < size) {
	        	key_value_line = line.split(split_char);
	        	
	        	if (key_value_line.length > 1) {
	        		key = key_column == 0 ? null : key_value_line[key_column-1];
		        	value = key_value_line[value_column-1]; 
	        	}
	        	else
	        		value = key_value_line[0];
	        
		        dataset.add(new Tuple2<>(key, value));
	        }
        } 
        catch (IOException e) { e.printStackTrace(); } 
        finally { 
        	try { reader.close(); } 
        	catch (IOException e) { e.printStackTrace(); } 
        }

        mapReduceAlgo.setFromRDD(mapReduceAlgo.parallelize(dataset));
	}
	
	public void createRandomRDD(String keyType, String valueType, int size) {		
		Random rnd = new Random();
        ArrayList<Tuple2<String, String>> dataset = new ArrayList<>();

        String key = null;
        String value = null;

        for (int i = 0; i < size; i++) {
            switch (keyType) {
                case "String": key = RandomStringUtils.randomAlphabetic(3,7); break;
                case "Integer": key = ""+rnd.nextInt(500); break;
                case "Double": key = String.format("%.1f", rnd.nextDouble()*500); break;
                case "-": key = null; break;
            }

            switch (valueType) {
                case "String": value = RandomStringUtils.randomAlphabetic(3,7); break;
                case "Integer": value = ""+rnd.nextInt(500); break;
                case "Double": value = ""+String.format("%.1f", rnd.nextDouble()*500); break;
            }

            dataset.add(new Tuple2<>(key, value));
        }

        mapReduceAlgo.setFromRDD(mapReduceAlgo.parallelize(dataset));
	}

	public List<Tuple2<String, String>> getDataset() { 
		return mapReduceAlgo.getFromRDD().collect(); 
	}
	
	public List<Tuple2<String, String>>[] getDatasetPartitions() { 
        int numPartitions = mapReduceAlgo.getFromRDD().getNumPartitions();
        
		return mapReduceAlgo.getFromRDD()
				.collectPartitions(IntStream.range(0, numPartitions).toArray()); 
	}
	
	public void replaceFromRDD() {
		mapReduceAlgo.setFromRDD(mapReduceAlgo.getToRDD());
		mapReduceAlgo.setToRDD(null);
	}
	
	public int getNumExecutors() {
		return mapReduceAlgo.getFromRDD().getNumPartitions();
	}
}

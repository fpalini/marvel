package spark_visualizer.visualization.sparkfx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.spark.HashPartitioner;

import javafx.animation.*;
import javafx.geometry.Bounds;
import javafx.geometry.Point2D;
import javafx.scene.Group;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Alert;
import javafx.scene.control.Label;
import javafx.scene.shape.LineTo;
import javafx.scene.shape.MoveTo;
import javafx.util.Duration;
import javafx.util.Pair;
import scala.Tuple2;

public class DistributedSystemFx extends Group {

	public final static int MARGIN = 30;
	public final static int PADDING = 50;

	public final static int RDD_FROM = 0;
	public final static int RDD_TO = 1;

	private ArrayList<ExecutorFx> executors = new ArrayList<>();
	private ArrayList<Label> labels = new ArrayList<>();

	private int nExecutors;
	private int blocksize;
	
	private String system_name;

	private double height, width;
	public DistributedSystemFx(int nExecutors, int blocksize) {
		this.nExecutors = nExecutors;
		this.blocksize = blocksize;

		ExecutorFx executor;
		Label label;

		for (int i = 0; i < nExecutors; i++) {
			executor = new ExecutorFx(0, 0);
			executor.getFromRDD().setBlocksize(blocksize);
			executor.getToRDD().setBlocksize(blocksize);
			executors.add(executor);

			label = new Label("Executor #" + (i+1));
			labels.add(label);
		}

		relocate();

		getChildren().addAll(executors);
		getChildren().addAll(labels);
	}

	public void parallelize(List<Tuple2<String,String>> dataset) {
		createRDD(dataset);
		relocate();
	}
	
	/*
	 * It splits the dataset into blocks with size=blockSize, among the executors.
	 */
	public void createRDD(List<Tuple2<String,String>> dataset) {
	
		Iterator<Tuple2<String, String>> data_iterator = dataset.iterator();

		int recordCounter;
		
		while (data_iterator.hasNext()) 
			for (ExecutorFx executor : executors) {
				recordCounter = 0;
				
				while (data_iterator.hasNext() && recordCounter++ < blocksize) {
					Tuple2<String, String> keyValue = data_iterator.next();
					executor.addRecordFromRDD(new RecordFx(keyValue._1, keyValue._2));
				}
			}	
	}

	/*
	 * Move down the executor. It manages the resize of RDDs.
	 */
	public void relocate() {
		int cols = nExecutors < 6 ? nExecutors : 6;

		double maxHeight = 0;
		double maxWidth = 0;

		for (ExecutorFx executor : executors)
			if (maxHeight < executor.height())
				maxHeight = executor.height();
		
		for (ExecutorFx executor : executors)
			if (maxWidth < executor.width())
				maxWidth = executor.width();


		int label_padding = 20;

		ExecutorFx executor;
		Label label;
		int r, c;
		double x, y;

		for (int i = 0; i < nExecutors; i++) {
			r = i / cols;
			c = i % cols;

			x = c * (maxWidth + PADDING);
			y = r * (maxHeight + PADDING);

			executor = executors.get(i);
			label = labels.get(i);

			executor.setLayoutX(x);
			executor.setLayoutY(y);

			label.setLayoutX(x);
			label.setLayoutY(y - label_padding);
		}
	}

	
	/*
	 * It copies the elements of the current system, into a new system, without side-effects.
	 */
	public DistributedSystemFx copy() {
		DistributedSystemFx system = new DistributedSystemFx(nExecutors, blocksize);
		ArrayList<ExecutorFx> es = new ArrayList<>();
		ArrayList<Label> ls = new ArrayList<>();

		for (ExecutorFx e : executors)
			es.add(e.copy());

		for (Label l : labels) {
			Label l1 = new Label(l.getText());
			l1.setLayoutX(l.getLayoutX());
			l1.setLayoutY(l.getLayoutY());
			ls.add(l1);
		}

		system.executors = es;
		system.labels = ls;
		system.setHeight(height);
		system.setWidth(width);

		system.getChildren().clear();

		system.getChildren().addAll(es);
		system.getChildren().addAll(ls);

		return system;
	}
	
	public Transition min(boolean byKey) {	
		ParallelTransition systemTransition = new ParallelTransition();
		systemTransition.getChildren().add(search());
		SequentialTransition executorTransition;
		
		String overallMin = null;
	
		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			try {
				for (RecordFx record : executor.getFromRDD().getRecords()) 
					Double.parseDouble(record.getValue().toString());
			} catch (NumberFormatException e) {
				warning("Values must be numbers!");
				return new PauseTransition(Duration.millis(500));
			}
			
			executorTransition = new SequentialTransition();
			
			ArrayList<Pair<String, String>> min_values = new ArrayList<>();	
	
			for (RecordFx record : executor.getFromRDD().getRecords()) {
				int index = byKey ? indexByKeyOf(record, executor.getToRDD().getRecords()) : 0;
				
				if (byKey && index == -1) // new record key
				{	
					min_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
					
					executorTransition.getChildren().add(executor.addRecordToRDD(record.copy()));
				} else { // record key found
					
					// if toRDD is empty it is an aggregate not by key
					if (executor.getToRDD().getRecords().isEmpty()) {
						min_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
						executorTransition.getChildren().add(executor.addRecordToRDD(record.copy()));
						if (overallMin == null) overallMin = record.getValue().toString();
						else if (Double.parseDouble(overallMin) > Double.parseDouble(record.getValue().toString())) 
							overallMin = record.getValue().toString();
						
						continue;
					}
					
					RecordFx prevRecord = executor.getToRDD().getRecords().get(index);
					
					if (Double.parseDouble(min_values.get(index).getValue()) > Double.parseDouble(record.getValue().toString())) {
						min_values.set(index, new Pair<>(record.getKey().toString(), record.getValue().toString()));
					}
					
					if (Double.parseDouble(overallMin) > Double.parseDouble(record.getValue().toString())) 
						overallMin = record.getValue().toString();
					
					
					String key = null;
					if (!byKey) key = min_values.get(index).getKey();
					
					executorTransition.getChildren().add(textUpdate(prevRecord, key, min_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(executorTransition);
		}
		
		final String final_overallMin = overallMin;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The minimum value is: " + final_overallMin));
		
		currentTransition = systemTransition;
		
		return currentTransition;
	}
	
	public Transition max(boolean byKey) {	
		ParallelTransition systemTransition = new ParallelTransition();
		systemTransition.getChildren().add(search());
		SequentialTransition executorTransition;
		
		String overallMax = null;
	
		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			try {
				for (RecordFx record : executor.getFromRDD().getRecords()) 
					Double.parseDouble(record.getValue().toString());
			} catch (NumberFormatException e) {
				warning("Values must be numbers!");
				return new PauseTransition(Duration.millis(500));
			}
			
			executorTransition = new SequentialTransition();
			
			ArrayList<Pair<String, String>> max_values = new ArrayList<>();
	
			for (RecordFx record : executor.getFromRDD().getRecords()) {
				int index = byKey ? indexByKeyOf(record, executor.getToRDD().getRecords()) : 0;
				
				if (byKey && index == -1) // new record key
				{	
					max_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
					
					executorTransition.getChildren().add(executor.addRecordToRDD(record.copy()));
				} else { // record key found
					if (executor.getToRDD().getRecords().isEmpty()) {
						max_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
						executorTransition.getChildren().add(executor.addRecordToRDD(record.copy()));
						if (overallMax == null) overallMax = record.getValue().toString();
						else if (Double.parseDouble(overallMax) < Double.parseDouble(record.getValue().toString())) 
							overallMax = record.getValue().toString();
							
						continue;
					}
					
					RecordFx prevRecord = executor.getToRDD().getRecords().get(index);
					
					if (Double.parseDouble(max_values.get(index).getValue()) < Double.parseDouble(record.getValue().toString()))
						max_values.set(index, new Pair<>(record.getKey().toString(), record.getValue().toString()));
					
					if (Double.parseDouble(overallMax) < Double.parseDouble(record.getValue().toString())) 
						overallMax = record.getValue().toString();
					
					
					String key = null;
					if (!byKey) key = max_values.get(index).getKey();
					
					executorTransition.getChildren().add(textUpdate(prevRecord, key, max_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(executorTransition);
		}
		
		final String final_overallMax = overallMax;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The maximum value is: " + final_overallMax));
		
		currentTransition = systemTransition;
		
		return currentTransition;
	}
	
	public Transition sum(boolean byKey) {
		ParallelTransition systemTransition = new ParallelTransition(search());
		SequentialTransition executorTransition;
		
		String overallSum = "0";
	
		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			try {
				for (RecordFx record : executor.getFromRDD().getRecords()) 
					Double.parseDouble(record.getValue().toString());
			} catch (NumberFormatException e) {
				warning("Values must be numbers!");
				return new PauseTransition(Duration.millis(500));
			}
			
			executorTransition = new SequentialTransition();
			
			ArrayList<Pair<String, String>> sum_values = new ArrayList<>();
	
			for (RecordFx record : executor.getFromRDD().getRecords()) {
				int index = byKey ? indexByKeyOf(record, executor.getToRDD().getRecords()) : 0;
				
				if (byKey && index == -1) // new record key
				{	
					sum_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
					
					executorTransition.getChildren().add(executor.addRecordToRDD(record.copy()));
				} else { // record key found			
					if (executor.getToRDD().getRecords().isEmpty()) {
						RecordFx r = new RecordFx(null, record.getValue().toString());
						
						sum_values.add(new Pair<String, String>(null, r.getValue().toString()));
						executorTransition.getChildren().add(executor.addRecordToRDD(r));
						overallSum = Double.parseDouble(overallSum) + 
								Double.parseDouble(record.getValue().toString()) + "";
						continue;
					}
					
					RecordFx prevRecord = executor.getToRDD().getRecords().get(index);
					
					sum_values.set(index, new Pair<>(record.getKey().toString(), 
							Double.parseDouble(sum_values.get(index).getValue()) + 
							Double.parseDouble(record.getValue().toString()) +""));
					
					overallSum = Double.parseDouble(overallSum) + 
							Double.parseDouble(record.getValue().toString()) + "";
					
					executorTransition.getChildren().add(textUpdate(prevRecord, null, sum_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(executorTransition);
		}
		
		final String final_overallSum = overallSum;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The overall sum is: " + final_overallSum));
		
		currentTransition = systemTransition;
		
		return currentTransition;
	}

	public Transition count(boolean byKey) {
		ParallelTransition systemTransition = new ParallelTransition(search());
		SequentialTransition executorTransition;
		
		String overallCount = "0";
	
		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			executorTransition = new SequentialTransition();
			
			ArrayList<Pair<String, String>> count_values = new ArrayList<>();
	
			for (RecordFx record : executor.getFromRDD().getRecords()) {
				int index = byKey ? indexByKeyOf(record, executor.getToRDD().getRecords()) : 0;
				
				if (byKey && index == -1) // new record key
				{	
					RecordFx r = new RecordFx(record.getKey().toString(), "1");
					count_values.add(new Pair<String, String>(r.getKey().toString(), r.getValue().toString()));
					
					executorTransition.getChildren().add(executor.addRecordToRDD(r));
				} else { // record key found			
					if (executor.getToRDD().getRecords().isEmpty()) {
						RecordFx r = new RecordFx(null, "1");
						
						count_values.add(new Pair<String, String>(null, "1"));
						executorTransition.getChildren().add(executor.addRecordToRDD(r));
						overallCount = Integer.parseInt(overallCount) + 1 + "";
						continue;
					}
					
					RecordFx prevRecord = executor.getToRDD().getRecords().get(index);
					
					count_values.set(index, new Pair<>(record.getKey().toString(), Integer.parseInt(count_values.get(index).getValue()) + 1 + ""));
					
					overallCount = Integer.parseInt(overallCount) + 1 + "";
					
					executorTransition.getChildren().add(textUpdate(prevRecord, null, count_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(executorTransition);
		}
		
		final String final_overallCount = overallCount;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The overall count is: " + final_overallCount));
		
		currentTransition = systemTransition;
		
		return currentTransition;
	}
	
	private Transition currentTransition;
	
	public Transition reduceByKey(String operation) {
		
		ArrayList<RDDPartitionFx> temps = new ArrayList<>();
		ArrayList<RDDPartitionFx> fromRDDs = new ArrayList<>();executors.get(0).getFromRDD();
		ArrayList<RDDPartitionFx> toRDDs = new ArrayList<>();executors.get(0).getToRDD();
		
		for (int e = 0; e < nExecutors; e++) {
			temps.add(executors.get(e).addTempRDD());
			fromRDDs.add(executors.get(e).getFromRDD());
			toRDDs.add(executors.get(e).getToRDD());
			
			executors.get(e).setToRDD(temps.get(e));
		}
		
		relocate();
		
		Transition systemTransition = new ParallelTransition(search(), operationToTransition(operation, true));
		
		systemTransition.setOnFinished(
			(event1) -> {				
				Transition transition = shuffle();
				transition.setRate(currentTransition.getRate());
				currentTransition = transition;
				
				transition.setOnFinished((event2) -> {
					for (int e = 0; e < nExecutors; e++) {
						executors.get(e).setFromRDD(temps.get(e));
						executors.get(e).setToRDD(toRDDs.get(e));
					}
					
					Transition seqTransition = new SequentialTransition(operationToTransition(operation, false), new PauseTransition(Duration.millis(1000)));
					seqTransition.setRate(currentTransition.getRate());
					currentTransition = seqTransition;
					
					seqTransition.setOnFinished((event) -> {
						for (int e = 0; e < nExecutors; e++) {
							executors.get(e).removeTempRDD(temps.get(e));
							executors.get(e).setFromRDD(fromRDDs.get(e));
							executors.get(e).setToRDD(toRDDs.get(e));
						}
						
						relocate();
					});
					seqTransition.play();
				});
				transition.play();
			}
		);
		
		currentTransition = systemTransition;
		
		return currentTransition;
	}
	
	private Transition operationToTransition(String operation, boolean isFirst) {
		switch (operation) {
			case "Count": return isFirst ? count(true) : sum(true);
			case "Min": return min(true);
			case "Max": return max(true);
			case "Sum": return sum(true);
		}
		
		return null;
	}
	
	private Transition shuffle() {
		ParallelTransition systemTransition = new ParallelTransition();
		SequentialTransition executorTransition;
		
		HashPartitioner sparkPartitioner = new HashPartitioner(nExecutors); 
	     
	    HashMap<Integer, ArrayList<RecordFx>> partitionsMap = new HashMap<>(); // records associated to new partitions
	    HashMap<Integer, ArrayList<RecordFx>> executorsMap = new HashMap<>(); // records_paste associated to old partitions
	    LinkedHashMap<RecordFx, RecordFx> recordsMap = new LinkedHashMap<>(); // record -> record_paste
	    LinkedHashMap<RecordFx, RecordFx> recordsSortMap = new LinkedHashMap<>(); // record_paste -> record_sort
	    HashMap<RecordFx, Point2D> newCoordsMap = new HashMap<>(); // record_paste -> new_coords
	     
	    // Computation of the partitions
	    for (int e = 0; e < executors.size(); e++)  
	    	for (RecordFx record : executors.get(e).getToRDD().getRecords()) { 
	    		int partition = sparkPartitioner.getPartition(record.getKey().toString()); 
	    		
	    		if (partitionsMap.get(partition) == null) partitionsMap.put(partition, new ArrayList<>());
	    		
	    		partitionsMap.get(partition).add(record);
	    	}
	    
	    // Paste of the records on the system
	    for (int e = 0; e < nExecutors; e++)
	    	for (RecordFx record : executors.get(e).getToRDD().getRecords()) {
	    		record.setVisible(false);
	    		RecordFx record_paste = record.copy();
	    		getChildren().add(record_paste);
	    		Bounds record_copy_bounds = getSystemBounds(record);
	    		record_paste.setLayoutX(record_copy_bounds.getMinX());
	    		record_paste.setLayoutY(record_copy_bounds.getMinY());
	    		recordsMap.put(record, record_paste);
	    		
	    		if (executorsMap.get(e) == null) executorsMap.put(e, new ArrayList<>());
	    		
	    		executorsMap.get(e).add(record_paste);	
	    	}
		
	    // Sorting of the records (per partition) and fade in of them
	    for (int e = 0; e < executors.size(); e++) {
	    	executors.get(e).getToRDD().clear();
	    	
	    	
	    	if (partitionsMap.get(e) == null) continue;
	    	
	    	partitionsMap.get(e).sort((r1, r2) -> r1.getKey().compareTo(r2.getKey()));
	    	
	    	for (RecordFx record : partitionsMap.get(e)) {
	    		RecordFx record_sort = record.copy();
	    		executors.get(e).addRecordToRDD(record_sort);
	    		record_sort.setVisible(false);
	    		
	    		double byX = getSystemBounds(record_sort).getMinX() + 
	    						getSystemBounds(record_sort).getWidth()/2 - 
	    							(getSystemBounds(recordsMap.get(record)).getMinX() + 
	    									getSystemBounds(recordsMap.get(record)).getWidth()/2);
	    		
	    		double byY = getSystemBounds(record_sort).getMinY() + 
	    						getSystemBounds(record_sort).getHeight()/2 - 
	    							(getSystemBounds(recordsMap.get(record)).getMinY() + 
	    									getSystemBounds(recordsMap.get(record)).getHeight()/2);
	    		
	    		newCoordsMap.put(recordsMap.get(record), new Point2D(byX, byY));
	    		recordsSortMap.put(recordsMap.get(record), record_sort);
	    	}
	    }
	    
	    // Translation of the records
	    for (int e = 0; e < nExecutors; e++) {
	    	executorTransition = new SequentialTransition(new PauseTransition(Duration.millis(300)));
	    	
	    	for (RecordFx record_paste : executorsMap.get(e)) {
	    		TranslateTransition transTransition = new TranslateTransition(Duration.millis(FieldFx.ANIMATION_MS), record_paste);
				
				transTransition.setByX(newCoordsMap.get(record_paste).getX());
				transTransition.setByY(newCoordsMap.get(record_paste).getY());
				
				executorTransition.getChildren().add(record_paste.getFadeIn());
				executorTransition.getChildren().add(transTransition);
				
				transTransition.setOnFinished((event) -> {
		    		record_paste.setVisible(false);
		    		recordsSortMap.get(record_paste).setVisible(true);
		    	});
	    	} 
	    	
	    	systemTransition.getChildren().add(executorTransition);
	    }
	    
	    currentTransition = systemTransition;
		
		return currentTransition;
	}			

	private int indexByKeyOf(RecordFx record, ArrayList<RecordFx> records) {
		int index = -1;
		
		for (int r = 0; r < records.size(); r++)
			if (record.getKey().equals(records.get(r).getKey()))
				index = r;
		
		return index;
	}
	
	public Transition swap() {

		ParallelTransition parTransition = new ParallelTransition();
		parTransition.getChildren().add(search());
		SequentialTransition swapTransition;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;

			swapTransition = new SequentialTransition();

			for (RecordFx record : executor.getFromRDD().getRecords()) {
				RecordFx record_copy = record.copy();
				record_copy.swap();
				
				swapTransition.getChildren().add(executor.addRecordToRDD(record_copy));
			}

			parTransition.getChildren().add(swapTransition);
		}
		
		currentTransition = parTransition;
		
		return currentTransition;
	}

	public Transition filter(String condition, String value) {

		ParallelTransition parTransition = new ParallelTransition();
		parTransition.getChildren().add(search());
		SequentialTransition filterTransition;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;

			filterTransition = new SequentialTransition();

			for (RecordFx record : executor.getFromRDD().getRecords()) {
				if (filterEval(condition, record.getValue().toString(), value))
				{
					Transition addTransition = executor.addRecordToRDD(record.copy());

					filterTransition.getChildren().add(addTransition);
				}
				else
					filterTransition.getChildren().add(new PauseTransition(Duration.millis(2 * FieldFx.ANIMATION_MS)));
			}

			parTransition.getChildren().add(filterTransition);
		}
		
		currentTransition = parTransition;
		
		return currentTransition;
	}

	private boolean filterEval(String condition, String value1, String value2) {
		Integer value;

		switch (condition) {
		case ">":
			value = value1.equals("null") ? Integer.MIN_VALUE : Integer.parseInt(value1);
			return value > Integer.parseInt(value2);
		case "<":
			value = value1.equals("null") ? Integer.MAX_VALUE : Integer.parseInt(value1);
			return value < Integer.parseInt(value2);
		case "=":
			if (value1.equals("null"))
				return false;
			return value1.equals(value2);
		case "!=":
			if (value1.equals("null"))
				return true;
			return !value1.equals(value2);
		default:
			return false;
		}
	}

	private Transition textUpdate(RecordFx record, String keyText, String valueText) {
		Timeline timeline = new Timeline();

		if (keyText != null) {
			KeyValue kv1 = new KeyValue(record.getKey().getLabel().textProperty(), keyText);
			KeyFrame kf1 = new KeyFrame(Duration.millis(FieldFx.ANIMATION_MS), kv1);

			timeline.getKeyFrames().add(kf1);
		}

		KeyValue kv2 = new KeyValue(record.getValue().getLabel().textProperty(), valueText);
		KeyFrame kf2 = new KeyFrame(Duration.millis(FieldFx.ANIMATION_MS), kv2);

		timeline.getKeyFrames().add(kf2);
		
		return new SequentialTransition(timeline, new PauseTransition(Duration.millis(FieldFx.ANIMATION_MS)));
	}
	
	public void overwriteFromRDD() {
		for (ExecutorFx executor : executors) {
			executor.getFromRDD().clear();
			
			for (RecordFx record : executor.getToRDD().getRecords())
				executor.addRecordFromRDD(record.copy());
			
			executor.getToRDD().clear();
		}
	}

	public Transition search() {
		ParallelTransition systemTransition = new ParallelTransition();
		SequentialTransition executorTransition;
		
		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			executorTransition = new SequentialTransition();

			for (RecordFx record : executor.getFromRDD().getRecords())
				executorTransition.getChildren().add(record.getColorChange());

			systemTransition.getChildren().add(executorTransition);
		}

		return systemTransition;
	}

	public static void information(String message) {
		Alert alert = new Alert(Alert.AlertType.INFORMATION);
		alert.setHeaderText(message);

		alert.show();
	}
	
	public static void warning(String message) {
		Alert alert = new Alert(Alert.AlertType.WARNING);
		alert.setTitle("Warning");
		alert.setHeaderText(message);

		alert.showAndWait();
	}

	public ArrayList<ExecutorFx> getExecutors() { return executors; }

	public double height() { return height; }

	public double width() { return width; }

	public void setHeight(double height) { this.height = height; }

	public void setWidth(double width) { this.width = width; }
	
	public void setSystemName(String name) { system_name = name; };
	
	@Override
	public String toString() {
		return system_name;
	}
	
	public static class MoveToAbs extends MoveTo {

	    public MoveToAbs( Node node) {
	        super( node.getLayoutBounds().getWidth() / 2, node.getLayoutBounds().getHeight() / 2);
	    }

	}

	public static class LineToAbs extends LineTo {

	    public LineToAbs( Node node, double x, double y) {
	        super( x - node.getLayoutX() + node.getLayoutBounds().getWidth() / 2, y - node.getLayoutY() + node.getLayoutBounds().getHeight() / 2);
	    }

	}
	
	private Bounds getSystemBounds(Node node) {
		Parent parent = node.getParent();
		Bounds bounds = node.getBoundsInParent();
		
		while (!parent.getClass().equals(DistributedSystemFx.class)) {
			bounds = parent.localToParent(bounds);
			parent = parent.getParent();
			if (parent == null)
				System.out.println();
		}
		
		return bounds;
	}
	
	public Transition getCurrentTransition() {
		return currentTransition;
	}
}

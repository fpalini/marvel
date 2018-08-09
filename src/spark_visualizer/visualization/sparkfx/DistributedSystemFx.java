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
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.paint.Color;
import javafx.scene.shape.LineTo;
import javafx.scene.shape.MoveTo;
import javafx.util.Duration;
import javafx.util.Pair;
import scala.Tuple2;

public class DistributedSystemFx extends Group {

	public final static int PADDING = 50;

	private ArrayList<NodeFx> nodes = new ArrayList<>();
	private ArrayList<Label> labels = new ArrayList<>();

	private int nNodes;
	private int blocksize;
	
	private String system_name;

	private double height, width;
	public DistributedSystemFx(int nNodes, int blocksize) {
		this.nNodes = nNodes;
		this.blocksize = blocksize;

		NodeFx node;
		Label label;

		for (int i = 0; i < nNodes; i++) {
			node = new NodeFx(0, 0);
			node.getFromRDD().setBlocksize(blocksize);
			node.getToRDD().setBlocksize(blocksize);
			nodes.add(node);

			label = new Label("Node #" + (i+1));
			labels.add(label);
		}

		relocate();

		getChildren().addAll(nodes);
		getChildren().addAll(labels);
	}

	public void parallelize(List<Tuple2<String,String>> dataset) {
		createRDD(dataset);
		relocate();
	}
	
	/*
	 * It splits the dataset into blocks with size=blockSize, among the nodes.
	 */
	public void createRDD(List<Tuple2<String,String>> dataset) {
	
		Iterator<Tuple2<String, String>> data_iterator = dataset.iterator();

		int recordCounter;
		
		while (data_iterator.hasNext()) 
			for (NodeFx node : nodes) {
				recordCounter = 0;
				
				while (data_iterator.hasNext() && recordCounter++ < blocksize) {
					Tuple2<String, String> keyValue = data_iterator.next();
					node.addRecordFromRDD(new RecordFx(keyValue._1, keyValue._2));
				}
			}	
	}

	/*
	 * Move down the node. It manages the resize of RDDs.
	 */
	public void relocate() {
		int cols = nNodes < 6 ? nNodes : 6;

		double maxHeight = 0;
		double maxWidth = 0;

		for (NodeFx node : nodes)
			if (maxHeight < node.height())
				maxHeight = node.height();
		
		for (NodeFx node : nodes)
			if (maxWidth < node.width())
				maxWidth = node.width();


		int label_padding = 20;

		NodeFx node;
		Label label;
		int r, c;
		double x, y;

		for (int i = 0; i < nNodes; i++) {
			r = i / cols;
			c = i % cols;

			x = c * (maxWidth + PADDING);
			y = r * (maxHeight + PADDING);

			node = nodes.get(i);
			label = labels.get(i);

			node.setLayoutX(x);
			node.setLayoutY(y);

			label.setLayoutX(x);
			label.setLayoutY(y - label_padding);
		}
	}

	
	/*
	 * It copies the elements of the current system, into a new system, without side-effects.
	 */
	public DistributedSystemFx copy() {
		DistributedSystemFx system = new DistributedSystemFx(nNodes, blocksize);
		ArrayList<NodeFx> es = new ArrayList<>();
		ArrayList<Label> ls = new ArrayList<>();

		for (NodeFx n : nodes)
			es.add(n.copy());

		for (Label l : labels) {
			Label l1 = new Label(l.getText());
			l1.setLayoutX(l.getLayoutX());
			l1.setLayoutY(l.getLayoutY());
			ls.add(l1);
		}

		system.nodes = es;
		system.labels = ls;
		system.setHeight(height);
		system.setWidth(width);

		system.getChildren().clear();

		system.getChildren().addAll(es);
		system.getChildren().addAll(ls);

		return system;
	}
	
	public Transition min(boolean byKey, boolean onKey) {	
		ParallelTransition systemTransition = new ParallelTransition();
		systemTransition.getChildren().add(search());
		SequentialTransition nodeTransition;
		
		String overallMin = null;
	
		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;
			
			nodeTransition = new SequentialTransition();
			
			ArrayList<Pair<String, String>> min_values = new ArrayList<>();	
	
			for (RecordFx record : node.getFromRDD().getRecords()) {
				int index = byKey ? indexByKeyOf(record, node.getToRDD().getRecords()) : 0;
				
				if (byKey && index == -1) // new record key
				{	
					min_values.add(new Pair<>(record.getKey().toString(), record.getValue().toString()));
					
					nodeTransition.getChildren().add(node.addRecordToRDD(record.copy()));
				} else { // record key found
					String element1 = onKey ? record.getKey().toString() : record.getValue().toString();
					
					// if toRDD is empty it is an aggregate not by key
					if (node.getToRDD().getRecords().isEmpty()) {
						min_values.add(new Pair<>(record.getKey().toString(), record.getValue().toString()));
						nodeTransition.getChildren().add(node.addRecordToRDD(record.copy()));
						if (overallMin == null) overallMin = element1;
						else if (Double.parseDouble(overallMin) > Double.parseDouble(element1)) 
							overallMin = element1;
						
						continue;
					}
					
					RecordFx prevRecord = node.getToRDD().getRecords().get(index);
					
					String element2 = onKey ? min_values.get(index).getKey() : min_values.get(index).getValue();
					
					if (Double.parseDouble(element2) > Double.parseDouble(element1)) {
						min_values.set(index, new Pair<>(record.getKey().toString(), record.getValue().toString()));
					}
					
					if (Double.parseDouble(overallMin) > Double.parseDouble(element1)) 
						overallMin = element1;
					
					
					String key = null;
					if (!byKey) key = min_values.get(index).getKey();
					
					nodeTransition.getChildren().add(textUpdate(prevRecord, key, min_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(nodeTransition);
		}
		
		final String final_overallMin = overallMin;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The minimum value is: " + final_overallMin));
		
		currentTransition = systemTransition;
		
		return currentTransition;
	}
	
	public Transition max(boolean byKey, boolean onKey) {	
		ParallelTransition systemTransition = new ParallelTransition();
		systemTransition.getChildren().add(search());
		SequentialTransition nodeTransition;
		
		String overallMax = null;
	
		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;
			
			nodeTransition = new SequentialTransition();
			
			ArrayList<Pair<String, String>> max_values = new ArrayList<>();
	
			for (RecordFx record : node.getFromRDD().getRecords()) {
				int index = byKey ? indexByKeyOf(record, node.getToRDD().getRecords()) : 0;
				String element1 = onKey ? record.getKey().toString() : record.getValue().toString();
				
				if (byKey && index == -1) // new record key
				{	
					max_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
					
					nodeTransition.getChildren().add(node.addRecordToRDD(record.copy()));
				} else { // record key found
					if (node.getToRDD().getRecords().isEmpty()) {
						max_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
						nodeTransition.getChildren().add(node.addRecordToRDD(record.copy()));
						if (overallMax == null) overallMax = element1;
						else if (Double.parseDouble(overallMax) < Double.parseDouble(element1)) 
							overallMax = element1;
							
						continue;
					}
					
					RecordFx prevRecord = node.getToRDD().getRecords().get(index);
					
					String element2 = onKey ? max_values.get(index).getKey() : max_values.get(index).getValue();
					
					if (Double.parseDouble(element2) < Double.parseDouble(element1))
						max_values.set(index, new Pair<>(record.getKey().toString(), record.getValue().toString()));
					
					if (Double.parseDouble(overallMax) < Double.parseDouble(element1)) 
						overallMax = element1;
					
					
					String key = null;
					if (!byKey) key = max_values.get(index).getKey();
					
					nodeTransition.getChildren().add(textUpdate(prevRecord, key, max_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(nodeTransition);
		}
		
		final String final_overallMax = overallMax;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The maximum value is: " + final_overallMax));
		
		currentTransition = systemTransition;
		
		return currentTransition;
	}
	
	public Transition sum(boolean byKey, boolean onKey) {
		ParallelTransition systemTransition = new ParallelTransition(search());
		SequentialTransition nodeTransition;
		
		String overallSum = "0";
	
		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;
			
			nodeTransition = new SequentialTransition();
			
			ArrayList<Pair<String, String>> sum_values = new ArrayList<>();
	
			for (RecordFx record : node.getFromRDD().getRecords()) {
				int index = byKey ? indexByKeyOf(record, node.getToRDD().getRecords()) : 0;
				
				if (byKey && index == -1) // new record key
				{	
					sum_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
					
					nodeTransition.getChildren().add(node.addRecordToRDD(record.copy()));
				} else { // record key found	
					String element = onKey ? record.getKey().toString() : record.getValue().toString();
					
					if (node.getToRDD().getRecords().isEmpty()) {
						RecordFx r = new RecordFx(null, element);
						
						sum_values.add(new Pair<String, String>(null, r.getValue().toString()));
						nodeTransition.getChildren().add(node.addRecordToRDD(r));
						overallSum = Double.parseDouble(overallSum) + 
								Double.parseDouble(element) + "";
						continue;
					}
					
					RecordFx prevRecord = node.getToRDD().getRecords().get(index);
					
					
					
					sum_values.set(index, new Pair<>(record.getKey().toString(), 
							Double.parseDouble(sum_values.get(index).getValue()) + 
							Double.parseDouble(element) +""));
					
					overallSum = Double.parseDouble(overallSum) + 
							Double.parseDouble(element) + "";
					
					nodeTransition.getChildren().add(textUpdate(prevRecord, null, sum_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(nodeTransition);
		}
		
		final String final_overallSum = overallSum;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The overall sum is: " + final_overallSum));
		
		currentTransition = systemTransition;
		
		return currentTransition;
	}

	public Transition count(boolean byKey) {
		ParallelTransition systemTransition = new ParallelTransition(search());
		SequentialTransition nodeTransition;
		
		String overallCount = "0";
	
		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;
			
			nodeTransition = new SequentialTransition();
			
			ArrayList<Pair<String, String>> count_values = new ArrayList<>();
	
			for (RecordFx record : node.getFromRDD().getRecords()) {
				int index = byKey ? indexByKeyOf(record, node.getToRDD().getRecords()) : 0;
				
				if (byKey && index == -1) // new record key
				{	
					RecordFx r = new RecordFx(record.getKey().toString(), "1");
					count_values.add(new Pair<String, String>(r.getKey().toString(), r.getValue().toString()));
					
					nodeTransition.getChildren().add(node.addRecordToRDD(r));
				} else { // record key found			
					if (node.getToRDD().getRecords().isEmpty()) {
						RecordFx r = new RecordFx(null, "1");
						
						count_values.add(new Pair<String, String>(null, "1"));
						nodeTransition.getChildren().add(node.addRecordToRDD(r));
						overallCount = Integer.parseInt(overallCount) + 1 + "";
						continue;
					}
					
					RecordFx prevRecord = node.getToRDD().getRecords().get(index);
					
					count_values.set(index, new Pair<>(record.getKey().toString(), Integer.parseInt(count_values.get(index).getValue()) + 1 + ""));
					
					overallCount = Integer.parseInt(overallCount) + 1 + "";
					
					nodeTransition.getChildren().add(textUpdate(prevRecord, null, count_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(nodeTransition);
		}
		
		final String final_overallCount = overallCount;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The overall count is: " + final_overallCount));
		
		currentTransition = systemTransition;
		
		return currentTransition;
	}
	
	private Transition currentTransition;
	
	public Transition reduceByKey(String operation, Button done_button) {
		
		ArrayList<RDDPartitionFx> temps = new ArrayList<>();
		ArrayList<RDDPartitionFx> fromRDDs = new ArrayList<>();
		ArrayList<RDDPartitionFx> toRDDs = new ArrayList<>();
		
		for (int n = 0; n < nNodes; n++) {
			temps.add(nodes.get(n).addTempRDD());
			fromRDDs.add(nodes.get(n).getFromRDD());
			toRDDs.add(nodes.get(n).getToRDD());
			
			nodes.get(n).setToRDD(temps.get(n));
		}
		
		relocate();
		
		Transition systemTransition = new ParallelTransition(search(), operationToTransition(operation, true));
		
		systemTransition.setOnFinished(
			(event1) -> {				
				Transition transition = shuffle();
				transition.setRate(currentTransition.getRate());
				currentTransition = transition;
				
				transition.setOnFinished((event2) -> {
					for (int n = 0; n < nNodes; n++) {
						nodes.get(n).setFromRDD(temps.get(n));
						nodes.get(n).setToRDD(toRDDs.get(n));
					}
					
					for (NodeFx node : nodes)
						if (node.getFromRDD().size() == 0)
			        		node.setColor(Color.CRIMSON);
			        	else
			        		node.setColor(Color.DEEPSKYBLUE);
					
					Transition seqTransition = new SequentialTransition(operationToTransition(operation, false), new PauseTransition(Duration.millis(1000)));
					seqTransition.setRate(currentTransition.getRate());
					currentTransition = seqTransition;
					
					seqTransition.setOnFinished((event) -> {
						for (int n = 0; n < nNodes; n++) {
							nodes.get(n).removeTempRDD(temps.get(n));
							nodes.get(n).setFromRDD(fromRDDs.get(n));
							nodes.get(n).setToRDD(toRDDs.get(n));
						}
						
						relocate();
						
						done_button.setDisable(false);
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
			case "Count": return isFirst ? count(true) : sum(true, false);
			case "Min": return min(true, false);
			case "Max": return max(true, false);
			case "Sum": return sum(true, false);
		}
		
		return null;
	}
	
	private Transition shuffle() {
		ParallelTransition systemTransition = new ParallelTransition();
		SequentialTransition nodeTransition;
		
		HashPartitioner sparkPartitioner = new HashPartitioner(nNodes); 
	     
	    HashMap<Integer, ArrayList<RecordFx>> partitionsMap = new HashMap<>(); // records associated to new partitions
	    HashMap<Integer, ArrayList<RecordFx>> nodesMap = new HashMap<>(); // records_paste associated to old partitions
	    LinkedHashMap<RecordFx, RecordFx> recordsMap = new LinkedHashMap<>(); // record -> record_paste
	    LinkedHashMap<RecordFx, RecordFx> recordsSortMap = new LinkedHashMap<>(); // record_paste -> record_sort
	    HashMap<RecordFx, Point2D> newCoordsMap = new HashMap<>(); // record_paste -> new_coords

	    // Computation of the partitions
	    for (int n = 0; n < nodes.size(); n++)  
	    	for (RecordFx record : nodes.get(n).getToRDD().getRecords()) { 
	    		int partition = sparkPartitioner.getPartition(record.getKey().toString()); 
	    		
	    		if (partitionsMap.get(partition) == null) partitionsMap.put(partition, new ArrayList<>());
	    		
	    		partitionsMap.get(partition).add(record);
	    	}
	    
	    // Paste of the records on the system
	    for (int n = 0; n < nNodes; n++) {
	    	nodesMap.put(n, new ArrayList<>());
	    	
	    	for (RecordFx record : nodes.get(n).getToRDD().getRecords()) {
	    		record.setVisible(false);
	    		RecordFx record_paste = record.copy();
	    		getChildren().add(record_paste);
	    		Bounds record_copy_bounds = getSystemBounds(record);
	    		record_paste.setLayoutX(record_copy_bounds.getMinX());
	    		record_paste.setLayoutY(record_copy_bounds.getMinY());
	    		recordsMap.put(record, record_paste);
	    		
	    		nodesMap.get(n).add(record_paste);	
	    	}
	    }
	    
	    ArrayList<Double> deltaHeights = new ArrayList<>();
	    
	    for (int n = 0; n < nNodes; n++)
	    	deltaHeights.add(getSystemBounds(nodes.get(n)).getMinY());
		
	    // Sorting of the records (per partition) and fade in of them
	    for (int n = 0; n < nodes.size(); n++) {
	    	nodes.get(n).getToRDD().clear();
	    	
	    	if (partitionsMap.get(n) == null) continue;
	    	
	    	partitionsMap.get(n).sort((r1, r2) -> r1.getKey().compareTo(r2.getKey()));

	    	for (RecordFx record : partitionsMap.get(n)) {
	    		RecordFx record_sort = record.copy();
	    		nodes.get(n).addRecordToRDD(record_sort);
	    		record_sort.setVisible(false);
	    		
	    		recordsSortMap.put(recordsMap.get(record), record_sort);
	    	}
	    }

	    relocate();
	    
	    for (int n = 0; n < nNodes; n++)
	    	deltaHeights.set(n, getSystemBounds(nodes.get(n)).getMinY() - deltaHeights.get(n));
	    
	    // Translation of the records
	    for (int n = 0; n < nNodes; n++) {
	    	nodeTransition = new SequentialTransition(new PauseTransition(Duration.millis(300)));
	    	
	    	for (RecordFx record_paste : nodesMap.get(n)) {
	    		record_paste.setLayoutY(record_paste.getLayoutY() + deltaHeights.get(n));
	    		
	    		TranslateTransition transTransition = new TranslateTransition(Duration.millis(FieldFx.ANIMATION_MS), record_paste);
	    		
	    		double byX = getSystemBounds(recordsSortMap.get(record_paste)).getMinX() +
						getSystemBounds(recordsSortMap.get(record_paste)).getWidth()/2 -
							(getSystemBounds(record_paste).getMinX() +
									getSystemBounds(record_paste).getWidth()/2);
	    		
	    		double byY = getSystemBounds(recordsSortMap.get(record_paste)).getMinY() +
						getSystemBounds(recordsSortMap.get(record_paste)).getHeight()/2 -
							(getSystemBounds(record_paste).getMinY() +
									getSystemBounds(record_paste).getHeight()/2);
	    		
				transTransition.setByX(byX);
				transTransition.setByY(byY);
				
				nodeTransition.getChildren().add(record_paste.getFadeIn());
				nodeTransition.getChildren().add(transTransition);
				
				transTransition.setOnFinished((event) -> {
		    		record_paste.setVisible(false);
		    		recordsSortMap.get(record_paste).setVisible(true);
		    	});
	    	} 
	    	
	    	systemTransition.getChildren().add(nodeTransition);
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

		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;

			swapTransition = new SequentialTransition();

			for (RecordFx record : node.getFromRDD().getRecords()) {
				RecordFx record_copy = record.copy();
				record_copy.swap();
				
				swapTransition.getChildren().add(node.addRecordToRDD(record_copy));
			}

			parTransition.getChildren().add(swapTransition);
		}
		
		currentTransition = parTransition;
		
		return currentTransition;
	}

	public Transition filter(String condition, String value, boolean onKey) {

		ParallelTransition parTransition = new ParallelTransition();
		parTransition.getChildren().add(search());
		SequentialTransition filterTransition;

		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;

			filterTransition = new SequentialTransition();

			for (RecordFx record : node.getFromRDD().getRecords()) {
				String element = onKey ? record.getKey().toString() : record.getValue().toString();
				
				if (filterEval(condition, element, value))
				{
					Transition addTransition = node.addRecordToRDD(record.copy());

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
		for (NodeFx node : nodes) {
			node.getFromRDD().clear();
			
			for (RecordFx record : node.getToRDD().getRecords())
				node.addRecordFromRDD(record.copy());
			
			node.getToRDD().clear();
		}
	}

	public Transition search() {
		ParallelTransition systemTransition = new ParallelTransition();
		SequentialTransition nodeTransition;
		
		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;
			
			nodeTransition = new SequentialTransition();

			for (RecordFx record : node.getFromRDD().getRecords())
				nodeTransition.getChildren().add(record.getColorChange());

			systemTransition.getChildren().add(nodeTransition);
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

	public ArrayList<NodeFx> getNodes() { return nodes; }

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

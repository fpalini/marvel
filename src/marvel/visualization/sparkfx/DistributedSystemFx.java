package marvel.visualization.sparkfx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeSet;

import org.apache.spark.HashPartitioner;

import javafx.animation.*;
import javafx.geometry.Bounds;
import javafx.scene.Group;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.shape.LineTo;
import javafx.scene.shape.MoveTo;
import javafx.scene.text.Text;
import javafx.util.Duration;
import javafx.util.Pair;
import scala.Tuple2;

public class DistributedSystemFx extends SystemFx {

	public final static int PADDING = 50;

	private ArrayList<NodeFx> nodes = new ArrayList<>();

	private int nNodes;
	private int blocksize;
	private int rowsize;

	private double height, width;
	public DistributedSystemFx(int nNodes, int blocksize, int rowsize) {
		this.nNodes = nNodes;
		this.blocksize = blocksize;
		this.rowsize = rowsize;
		
		NodeFx node;

		for (int i = 0; i < nNodes; i++) {
			node = new NodeFx(0, 0, i+1);
			node.getFromRDD().setBlocksize(blocksize);
			node.getToRDD().setBlocksize(blocksize);
			nodes.add(node);
		}

		relocate();

		getChildren().addAll(nodes);
	}

	public void createInput(List<Tuple2<String,String>> dataset) {
		createRDD(dataset);
		relocate();
	}
	
	/*
	 * It splits the dataset into blocks with size=blockSize, among the nodes.
	 */
	public void createRDD(List<Tuple2<String,String>> dataset) {
	
		Iterator<Tuple2<String, String>> data_iterator = dataset.iterator();
		
		for (Tuple2<String, String> t : dataset) {
			if (new Text(t._1).getLayoutBounds().getWidth() + 10 > FieldFx.get_width() ||
					new Text(t._2).getLayoutBounds().getWidth() + 10 > FieldFx.get_width()) {
				
				FieldFx.set_width(FieldFx.DEFAULT_WIDTH + 40);
				
				for (NodeFx n : nodes)
					n.recompute_width();
				
				break;
			}
		}

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
		int cols = nNodes < rowsize ? nNodes : rowsize;

		double maxHeight = 0;
		double maxWidth = 0;

		for (NodeFx node : nodes)
			if (maxHeight < node.height())
				maxHeight = node.height();
		
		for (NodeFx node : nodes)
			if (maxWidth < node.width())
				maxWidth = node.width();

		NodeFx node;
		int r, c;
		double x, y;

		for (int i = 0; i < nNodes; i++) {
			r = i / cols;
			c = i % cols;

			x = c * (maxWidth + PADDING);
			y = r * (maxHeight + PADDING);

			node = nodes.get(i);

			node.setLayoutX(x+20);
			node.setLayoutY(y+20);
		}
	}

	
	/*
	 * It copies the elements of the current system, into a new system, without side-effects.
	 */
	public DistributedSystemFx copy() {
		DistributedSystemFx system = new DistributedSystemFx(nNodes, blocksize, rowsize);
		ArrayList<NodeFx> es = new ArrayList<>();
		ArrayList<Label> ls = new ArrayList<>();

		for (NodeFx n : nodes)
			es.add(n.copy());

		system.nodes = es;
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
		RecordFx overallMinRecord = null;
	
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
						if (overallMin == null || 
								Double.parseDouble(overallMin) > Double.parseDouble(element1)) {
							overallMin = element1;
							overallMinRecord = record;
						}
						
						continue;
					}
					
					RecordFx prevRecord = node.getToRDD().getRecords().get(index);
					
					String element2 = onKey ? min_values.get(index).getKey() : min_values.get(index).getValue();
					
					if (Double.parseDouble(element2) > Double.parseDouble(element1)) {
						min_values.set(index, new Pair<>(record.getKey().toString(), record.getValue().toString()));
					}
					
					System.out.println(overallMin);
					
					if (overallMin == null || Double.parseDouble(overallMin) > Double.parseDouble(element1)) {
						overallMin = element1;
						overallMinRecord = record;
					}
					
					
					String key = null;
					if (!byKey) key = min_values.get(index).getKey();
					
					nodeTransition.getChildren().add(textUpdate(prevRecord, key, min_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(nodeTransition);
		}
		
		final String final_overallMin = overallMin;
		final RecordFx final_overallMinRecord = overallMinRecord;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The minimum value is: " + final_overallMin + "\nrecord: "+final_overallMinRecord));
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}
	
	public Transition max(boolean byKey, boolean onKey) {	
		ParallelTransition systemTransition = new ParallelTransition();
		systemTransition.getChildren().add(search());
		SequentialTransition nodeTransition;
		
		RecordFx overallMaxRecord = null;
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
						if (overallMax == null || 
								Double.parseDouble(overallMax) < Double.parseDouble(element1)) {
							overallMax = element1;
							overallMaxRecord = record;
						}
							
						continue;
					}
					
					RecordFx prevRecord = node.getToRDD().getRecords().get(index);
					
					String element2 = onKey ? max_values.get(index).getKey() : max_values.get(index).getValue();
					
					if (Double.parseDouble(element2) < Double.parseDouble(element1))
						max_values.set(index, new Pair<>(record.getKey().toString(), record.getValue().toString()));
					
					if (overallMax == null || Double.parseDouble(overallMax) < Double.parseDouble(element1)) {
						overallMax = element1;
						overallMaxRecord = record;
					}
					
					
					String key = null;
					if (!byKey) key = max_values.get(index).getKey();
					
					nodeTransition.getChildren().add(textUpdate(prevRecord, key, max_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(nodeTransition);
		}
		
		final String final_overallMax = overallMax;
		final RecordFx final_overallMaxRecord = overallMaxRecord;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The maximum value is: " + final_overallMax + "\nrecord: " + final_overallMaxRecord));
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}
	
	public Transition sum(boolean byKey, boolean onKey, boolean isInteger) {
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
						overallSum = isInteger ? Integer.parseInt(overallSum) + 
									Integer.parseInt(element) + "" : 
									Double.parseDouble(overallSum) + 
									Double.parseDouble(element) + "";
						continue;
					}
					
					RecordFx prevRecord = node.getToRDD().getRecords().get(index);
					
					String new_value = isInteger ? Integer.parseInt(sum_values.get(index).getValue()) + 
							Integer.parseInt(element) + "" : 
							Double.parseDouble(sum_values.get(index).getValue()) + 
							Double.parseDouble(element) + "";
					
					sum_values.set(index, new Pair<>(record.getKey().toString(), new_value));
					
					overallSum = isInteger ? Integer.parseInt(overallSum) + 
							Integer.parseInt(element) + "" : 
							Double.parseDouble(overallSum) + 
							Double.parseDouble(element) + "";
					
					nodeTransition.getChildren().add(textUpdate(prevRecord, null, sum_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(nodeTransition);
		}
		
		final String final_overallSum = overallSum;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The overall sum is: " + final_overallSum));
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
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
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}
	
	public Transition reduceByKey(String operation, Button done_button) {
		
		ArrayList<RDDPartitionFx> fromShuffle = new ArrayList<>();
		ArrayList<RDDPartitionFx> toShuffle = new ArrayList<>();
		ArrayList<RDDPartitionFx> fromRDDs = new ArrayList<>();
		ArrayList<RDDPartitionFx> toRDDs = new ArrayList<>();
		
		for (int n = 0; n < nNodes; n++) {
			fromRDDs.add(nodes.get(n).getFromRDD());
			toRDDs.add(nodes.get(n).getToRDD());
			if (nodes.size() > 1) {
				fromShuffle.add(nodes.get(n).addTempRDD());
				toShuffle.add(nodes.get(n).addTempRDD());
				
				nodes.get(n).setToRDD(fromShuffle.get(n));
			}
		}
		
		relocate();
		
		Transition systemTransition = operationToTransition(operation, true);
		
		if (nodes.size() > 1)
			systemTransition.setOnFinished(
				(event1) -> {			
					for (int n = 0; n < nNodes; n++) {
						nodes.get(n).setFromRDD(fromShuffle.get(n));
						nodes.get(n).setToRDD(toShuffle.get(n));
					}
					
					Transition transition = shuffle();
					transition.setRate(getSpeed());
					setCurrentTransition(transition);
					
					transition.setOnFinished((event2) -> {
						for (int n = 0; n < nNodes; n++) {
							nodes.get(n).setFromRDD(toShuffle.get(n));
							nodes.get(n).setToRDD(toRDDs.get(n));
						}
						
						for (NodeFx node : nodes)
							if (node.getFromRDD().size() == 0)
				        		node.setColor(NodeFx.RED);
				        	else
				        		node.setColor(NodeFx.GREEN);
						
						Transition seqTransition = operationToTransition(operation, false);
						seqTransition.setRate(getSpeed());
						setCurrentTransition(seqTransition);
						
						seqTransition.setOnFinished((event) -> {
							for (int n = 0; n < nNodes; n++) 
								nodes.get(n).setFromRDD(fromRDDs.get(n));
							
							relocate();
							
							done_button.setDisable(false);
						});
						seqTransition.play();
					});
					transition.play();
				}
			);
		else
			systemTransition.setOnFinished(event -> done_button.setDisable(false));
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}
	
	private Transition shuffle() {
		SequentialTransition systemTransition = new SequentialTransition();
		ParallelTransition nodeTransition;
		
		HashPartitioner sparkPartitioner = new HashPartitioner(nNodes); 
	     
	    HashMap<Integer, ArrayList<RecordFx>> partitionsMap = new HashMap<>(); // records associated to new partitions
	    HashMap<Integer, ArrayList<RecordFx>> nodesMap = new HashMap<>(); // record_paste associated to old partitions
	    LinkedHashMap<RecordFx, RecordFx> recordsMap = new LinkedHashMap<>(); // record -> record_paste
	    LinkedHashMap<RecordFx, RecordFx> recordsSortMap = new LinkedHashMap<>(); // record -> record_sort

	    // Computation of the partitions
	    for (int n = 0; n < nodes.size(); n++)  
	    	for (RecordFx record : nodes.get(n).getFromRDD().getRecords()) { 
	    		int partition = sparkPartitioner.getPartition(record.getKey().toString()); 
	    		
	    		if (partitionsMap.get(partition) == null) partitionsMap.put(partition, new ArrayList<>());
	    		
	    		partitionsMap.get(partition).add(record);
	    	}
	    
	    // Paste of the records on the system
	    for (int n = 0; n < nNodes; n++) {
	    	nodesMap.put(n, new ArrayList<>());
	    	
	    	for (RecordFx record : nodes.get(n).getFromRDD().getRecords()) {
	    		RecordFx record_paste = record.copy();
	    		record_paste.setOpacity(1);
	    		getChildren().add(record_paste);
	    		record.setVisible(false);
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
	    	if (partitionsMap.get(n) == null) continue;
	    	
	    	partitionsMap.get(n).sort((r1, r2) -> r1.getKey().compareTo(r2.getKey()));

	    	for (RecordFx record : partitionsMap.get(n)) {
	    		RecordFx record_sort = record.copy();
	    		record_sort.setOpacity(1);
	    		nodes.get(n).addRecordToRDD(record_sort);
	    		record_sort.setVisible(false);
	    		
	    		recordsSortMap.put(recordsMap.get(record), record_sort);
	    	}
	    }

	    relocate();
	    
	    for (int n = 0; n < nNodes; n++)
	    	deltaHeights.set(n, getSystemBounds(nodes.get(n)).getMinY() - deltaHeights.get(n));
	    
	    HashMap<String, ArrayList<RecordFx>> groupRecords = new HashMap<>();
	    
	   	for (int n = 0; n < nNodes; n++)
	   		for (RecordFx record_paste : nodesMap.get(n)) {
	   			if (groupRecords.get(record_paste.getKey().toString()) == null)
	   				groupRecords.put(record_paste.getKey().toString(), new ArrayList<>());
	   			
	   			groupRecords.get(record_paste.getKey().toString()).add(record_paste);
	   		}
	   	
	   	for (ArrayList<RecordFx> sameRecords : groupRecords.values()) {
	   		nodeTransition = new ParallelTransition(new PauseTransition(Duration.millis(300)));
	   		
	   		for (RecordFx record_paste : sameRecords) {
	    		record_paste.setLayoutY(record_paste.getLayoutY() + deltaHeights.get(findNode(nodesMap, record_paste)));
	    		
	    		TranslateTransition transTransition = new TranslateTransition(Duration.millis(3 * FieldFx.ANIMATION_MS), record_paste);
	    		
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
				
				nodeTransition.getChildren().add(transTransition);
				
				transTransition.setOnFinished((event) -> {
		    		recordsSortMap.get(record_paste).setVisible(true);
		    		record_paste.setVisible(false);
		    	});
	    	} 
	    	
	    	systemTransition.getChildren().add(nodeTransition);
	   	}
	    
	    setCurrentTransition(systemTransition);
		
		return systemTransition;
	}	
	
	private int findNode(HashMap<Integer, ArrayList<RecordFx>> nodesMap, RecordFx record) {
		for (int n = 0; n < nodesMap.size(); n++) {
			int index = nodesMap.get(n).indexOf(record);
			if (index != -1)
				return n;
		}
		
		return -1;
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
				FieldFx key = record_copy.getKey();
				record_copy.setKey(new FieldFx(record_copy.getValue().toString()));
				record_copy.setValue(new FieldFx(key.toString()));
				
				swapTransition.getChildren().add(node.addRecordToRDD(record_copy));
			}

			parTransition.getChildren().add(swapTransition);
		}
		
		parTransition.setOnFinished((event) -> getDoneButton().setDisable(false));
		
		setCurrentTransition(parTransition);
		
		return parTransition;
	}
	
	public Transition split() {
		ParallelTransition parTransition = new ParallelTransition();
		parTransition.getChildren().add(search());
		SequentialTransition splitTransition;

		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;

			splitTransition = new SequentialTransition();

			for (RecordFx record : node.getFromRDD().getRecords()) {
				String line = record.getValue().toString();
				ParallelTransition lineTransition = new ParallelTransition();
				for (String token : line.split(" ")) {
					RecordFx record_copy = record.copy();
					record_copy.setKey(new FieldFx(token.trim().toLowerCase()));
					record_copy.setValue(new FieldFx("1"));
					
					lineTransition.getChildren().add(node.addRecordToRDD(record_copy));
				}
				splitTransition.getChildren().add(lineTransition);
			}

			parTransition.getChildren().add(splitTransition);
		}
		
		parTransition.setOnFinished((event) -> getDoneButton().setDisable(false));
		
		setCurrentTransition(parTransition);
		
		return parTransition;
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
		
		parTransition.setOnFinished((event) -> getDoneButton().setDisable(false));
		
		setCurrentTransition(parTransition);
		
		return parTransition;
	}
	
	public void overwriteFromRDD() {
		for (NodeFx node : nodes) {
			node.getFromRDD().getChildren().remove(1, node.getFromRDD().size()+1);
			
			for (RecordFx record : node.getToRDD().getRecords())
				node.addRecordFromRDD(record.copy());
			
			node.getToRDD().getChildren().remove(1, node.getToRDD().size()+1);
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

	public ArrayList<NodeFx> getNodes() { return nodes; }

	public double height() { return height; }

	public double width() { return width; }

	public void setHeight(double height) { this.height = height; }

	public void setWidth(double width) { this.width = width; }
	
	

	@Override
	public Transition groupByKey() {
		ParallelTransition systemTransition = new ParallelTransition(search());
		SequentialTransition nodeTransition;
		TreeSet<String> values1, values2;
		
		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;
			
			nodeTransition = new SequentialTransition();
			
			ArrayList<Pair<String, String>> group_values = new ArrayList<>();
			HashMap<String, TreeSet<String>> groups = new HashMap<>();
	
			for (RecordFx record : node.getFromRDD().getRecords()) {
				int index = indexByKeyOf(record, node.getToRDD().getRecords());
				
				if (index == -1) // new record key
				{	
					groups.put(record.getKey().toString(), new TreeSet<>());
					values1 = groups.get(record.getKey().toString());
					values2 = stringToOrderedSet(record.getValue().toString());
					values1.addAll(values2);
					
					RecordFx r = new RecordFx(record.getKey().toString(), values1.toString().substring(1, values1.toString().length()-1));
					group_values.add(new Pair<String, String>(r.getKey().toString(), r.getValue().toString()));
					
					nodeTransition.getChildren().add(node.addRecordToRDD(r));
				} else { // record key found	
					
					RecordFx prevRecord = node.getToRDD().getRecords().get(index);
					
					values1 = groups.get(record.getKey().toString());
					values2 = stringToOrderedSet(record.getValue().toString());
					values1.addAll(values2);
					group_values.set(index, new Pair<>(record.getKey().toString(), values1.toString().substring(1, values1.toString().length()-1)));
										
					nodeTransition.getChildren().add(textUpdate(prevRecord, null, group_values.get(index).getValue()));
				}
			}
			
			systemTransition.getChildren().add(nodeTransition);
		}
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}

	@Override
	public Transition flatMapToPair() {
		ParallelTransition parTransition = new ParallelTransition();
		parTransition.getChildren().add(search());
		SequentialTransition splitTransition;
	
		for (NodeFx node : nodes) {
			if (node.getFromRDD().isEmpty()) continue;
	
			splitTransition = new SequentialTransition();
	
			for (RecordFx record : node.getFromRDD().getRecords()) {
				ParallelTransition lineTransition = new ParallelTransition();
				TreeSet<String> treeSet = stringToOrderedSet(record.getValue().toString());
				
				if (treeSet.size() < 2) {
					splitTransition.getChildren().add(new PauseTransition(Duration.millis(2 * FieldFx.ANIMATION_MS)));
					continue;
				}
				
				String[] array = treeSet.toArray(new String[0]);
				
				for (int i = 0; i < treeSet.size()-1; i++) 
					for (int j = i+1; j < treeSet.size(); j++) {
						RecordFx record_copy = record.copy();
						record_copy.setKey(new FieldFx(array[i]+", "+array[j]));
						record_copy.setValue(new FieldFx("1"));
					
						lineTransition.getChildren().add(node.addRecordToRDD(record_copy));
					}
				
				splitTransition.getChildren().add(lineTransition);
			}
	
			parTransition.getChildren().add(splitTransition);
		}
		
		parTransition.setOnFinished((event) -> getDoneButton().setDisable(false));
		
		setCurrentTransition(parTransition);
		
		return parTransition;
	}
	
	/*public static class MoveToAbs extends MoveTo {

	    public MoveToAbs( Node node) {
	        super( node.getLayoutBounds().getWidth() / 2, node.getLayoutBounds().getHeight() / 2);
	    }

	}

	public static class LineToAbs extends LineTo {

	    public LineToAbs( Node node, double x, double y) {
	        super( x - node.getLayoutX() + node.getLayoutBounds().getWidth() / 2, y - node.getLayoutY() + node.getLayoutBounds().getHeight() / 2);
	    }

	}*/
}

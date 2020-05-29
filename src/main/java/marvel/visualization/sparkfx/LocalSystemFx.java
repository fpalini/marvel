package marvel.visualization.sparkfx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import javafx.animation.ParallelTransition;
import javafx.animation.PauseTransition;
import javafx.animation.SequentialTransition;
import javafx.animation.Transition;
import javafx.geometry.Bounds;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.shape.Line;
import javafx.scene.text.Text;
import javafx.util.Duration;
import javafx.util.Pair;
import scala.Tuple2;

public class LocalSystemFx extends SystemFx {
	
	private ArrayList<RDDPartitionFx> structs;
	private HashMap<Tuple2<Integer, Integer>, ArrayList<Tuple2<Integer, Integer>>> parents; // (rdd_to_id, record_to_id) -> [(rdd_from_id, record_from_id)]
	private HashMap<Tuple2<Integer, Integer>, ArrayList<Line>> lines;                       // (rdd_to_id, record_to_id) -> [line]
	
	public LocalSystemFx() {
		structs = new ArrayList<>();
		parents = new HashMap<>();
		lines = new HashMap<>();				
	}
	
	public void createInput(List<Tuple2<String,String>> dataset) {
		
		Iterator<Tuple2<String, String>> data_iterator = dataset.iterator();
		
		for (Tuple2<String, String> t : dataset) {
			if (new Text(t._1).getLayoutBounds().getWidth() + 10 > FieldFx.get_width() ||
					new Text(t._2).getLayoutBounds().getWidth() + 10 > FieldFx.get_width()) {
				
				FieldFx.set_width(120);
				
				break;
			}
		}
		
		addStruct(new RDDPartitionFx());
		
		while (data_iterator.hasNext()) {
			Tuple2<String, String> keyValue = data_iterator.next();
			structs.get(0).addRecord(new RecordFx(keyValue._1, keyValue._2));
		}	
	}

	@Override
	public LocalSystemFx copy() {
		LocalSystemFx system = new LocalSystemFx();
		for (RDDPartitionFx rdd : structs)
			system.addStruct(rdd.copy());
		
		system.parents = new HashMap<>();
		system.lines = new HashMap<>();
		
		for (int i = 0; i < system.structs.size(); i++)
			for (int r = 0; r < system.structs.get(i).getRecords().size(); r++) {
				Tuple2<Integer, Integer> to = new Tuple2<Integer, Integer>(i, r);
				if (parents.get(to) != null) 
					system.parents.put(to, parents.get(to));
			}
		
		for (int i = 0; i < system.structs.size(); i++)
			for (int r = 0; r < system.structs.get(i).getRecords().size(); r++) {
				Tuple2<Integer, Integer> to = new Tuple2<Integer, Integer>(i, r);
				if (lines.get(to) != null) {
					system.lines.put(to, lines.get(to));
					system.structs.get(i).getRecords().get(r).setOnMouseClicked(event -> setVisibilityLines(to));
				}
			}
		
		for (ArrayList<Line> line_list: system.lines.values())
			for (Line l : line_list) {
				system.getChildren().add(l);
				l.setVisible(false);
			}
		
		for (Node n : getChildren())
			if (n instanceof Text)
				system.getChildren().add(new Text(((Text) n).getText()));
		
		return system;
	}

	public ArrayList<RDDPartitionFx> getStructs() {
		return structs;
	}
	
	public void addStruct(RDDPartitionFx struct) {
		double x = structs.size() > 0 ? structs.get(structs.size()-1).getLayoutX() + 100 + 2 * FieldFx.get_width() : 20;
		
		getChildren().add(struct);
		structs.add(struct);
		struct.setBlocksize(Integer.MAX_VALUE);
		
		struct.setLayoutY(50);
		struct.setLayoutX(x);
	}

	@Override
	public Transition swap() {
		ParallelTransition parTransition = new ParallelTransition();
		parTransition.getChildren().add(search());
		SequentialTransition swapTransition;
		
		RDDPartitionFx input = structs.get(structs.size()-1);
		
		addStruct(new RDDPartitionFx()); // destination struct
		
		RDDPartitionFx output = structs.get(structs.size()-1);

		swapTransition = new SequentialTransition();

		for (RecordFx record : input.getRecords()) {
			RecordFx record_copy = record.copy();
			FieldFx key = record_copy.getKey();
			record_copy.setKey(new FieldFx(record_copy.getValue().toString()));
			record_copy.setValue(new FieldFx(key.toString()));
			
			output.addRecord(record_copy);
			addLink(record, record_copy);
			
			swapTransition.getChildren().add(record_copy.getFadeIn());
		}

		parTransition.getChildren().add(swapTransition);
		
		parTransition.setOnFinished((event) -> getDoneButton().setDisable(false));
		
		setCurrentTransition(parTransition);
		
		return parTransition;
	}

	@Override
	public Transition filter(String condition, String value, boolean onKey) {

		ParallelTransition parTransition = new ParallelTransition();
		parTransition.getChildren().add(search());
		SequentialTransition filterTransition;
		
		RDDPartitionFx input = structs.get(structs.size()-1);
		
		addStruct(new RDDPartitionFx()); // destination struct
		
		RDDPartitionFx output = structs.get(structs.size()-1);

		filterTransition = new SequentialTransition();

		for (RecordFx record : input.getRecords()) {
			String element = onKey ? record.getKey().toString() : record.getValue().toString();
			
			if (filterEval(condition, element, value))
			{
				RecordFx r = record.copy();
				output.addRecord(r);
				addLink(record, r);
				Transition addTransition = r.getFadeIn();

				filterTransition.getChildren().add(addTransition);
			}
			else
				filterTransition.getChildren().add(new PauseTransition(Duration.millis(2 * FieldFx.ANIMATION_MS)));
		}

		parTransition.getChildren().add(filterTransition);
		
		
		parTransition.setOnFinished((event) -> getDoneButton().setDisable(false));
		
		setCurrentTransition(parTransition);
		
		return parTransition;
	}
	
	@Override
	public Transition flatMapToPair() {
		ParallelTransition parTransition = new ParallelTransition();
		parTransition.getChildren().add(search());
		SequentialTransition splitTransition;

		splitTransition = new SequentialTransition();
		
		RDDPartitionFx input = structs.get(structs.size()-1);
		
		addStruct(new RDDPartitionFx()); // destination struct
		
		RDDPartitionFx output = structs.get(structs.size()-1);

		for (RecordFx record : input.getRecords()) {
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
					
					output.addRecord(record_copy);
					
					addLink(record, record_copy);
					
					lineTransition.getChildren().add(record_copy.getFadeIn());
				}
					
					
			splitTransition.getChildren().add(lineTransition);
		}

		parTransition.getChildren().add(splitTransition);
		
		
		parTransition.setOnFinished((event) -> getDoneButton().setDisable(false));
		
		setCurrentTransition(parTransition);
		
		return parTransition;
	}

	@Override
	public Transition split() {
		ParallelTransition parTransition = new ParallelTransition();
		parTransition.getChildren().add(search());
		SequentialTransition splitTransition;

		splitTransition = new SequentialTransition();
		
		RDDPartitionFx input = structs.get(structs.size()-1);
		
		addStruct(new RDDPartitionFx()); // destination struct
		
		RDDPartitionFx output = structs.get(structs.size()-1);

		for (RecordFx record : input.getRecords()) {
			String line = record.getValue().toString();
			ParallelTransition lineTransition = new ParallelTransition();
			for (String token : line.split(" ")) {
				RecordFx record_copy = record.copy();
				record_copy.setKey(new FieldFx(token.trim().toLowerCase()));
				record_copy.setValue(new FieldFx("1"));
				
				output.addRecord(record_copy);
				
				addLink(record, record_copy);
				
				lineTransition.getChildren().add(record_copy.getFadeIn());
			}
			splitTransition.getChildren().add(lineTransition);
		}

		parTransition.getChildren().add(splitTransition);
		
		
		parTransition.setOnFinished((event) -> getDoneButton().setDisable(false));
		
		setCurrentTransition(parTransition);
		
		return parTransition;
	}

	private void addLink(RecordFx record, RecordFx record_copy) {
		Bounds record_bounds = getSystemBounds(record);
		Bounds record_copy_bounds = getSystemBounds(record_copy);
		
		Line l = new Line();	
		l.setStartX(record_bounds.getMaxX());
		l.setStartY(record_bounds.getMaxY() - FieldFx.HEIGHT/2);
		l.setEndX(record_copy_bounds.getMinX());
		l.setEndY(record_copy_bounds.getMaxY() - FieldFx.HEIGHT/2);
		l.setVisible(false);
		l.setStrokeWidth(2);
		
		Tuple2<Integer, Integer> from = new Tuple2<Integer, Integer>(structs.size()-2, 
																		structs.get(structs.size()-2).indexOf(record));
		
		Tuple2<Integer, Integer> to = new Tuple2<Integer, Integer>(structs.size()-1, 
																		structs.get(structs.size()-1).indexOf(record_copy));
		
		if (parents.get(to) == null) {
			parents.put(to, new ArrayList<>());
			lines.put(to, new ArrayList<>());
		}
		
		parents.get(to).add(from);
		lines.get(to).add(l);
		
		record_copy.setOnMouseClicked(event -> setVisibilityLines(to));
		
		getChildren().add(l);
	}
	
	private void setVisibilityLines(Tuple2<Integer, Integer> root) {
		if (lines.get(root) == null) return;
		
		for (Line line : lines.get(root))
			line.setVisible(!line.isVisible());
		
		for (Tuple2<Integer, Integer> parent : parents.get(root))
			setVisibilityLines(parent);
	}
	
	@Override
	public Transition groupByKey() {
		ParallelTransition systemTransition = new ParallelTransition(search());
		SequentialTransition nodeTransition;
		
		nodeTransition = new SequentialTransition();
		
		ArrayList<Pair<String, String>> group_values = new ArrayList<>();
		
		RDDPartitionFx input = structs.get(structs.size()-1);
		
		addStruct(new RDDPartitionFx()); // destination struct
		
		RDDPartitionFx output = structs.get(structs.size()-1);
		
		HashMap<String, TreeSet<String>> groups = new HashMap<>();
		
		TreeSet<String> values;

		for (RecordFx record : input.getRecords()) {
			int index = indexByKeyOf(record, output.getRecords());
			
			if (index == -1) // new record key
			{	
				groups.put(record.getKey().toString(), new TreeSet<>());
				values = groups.get(record.getKey().toString());
				values.add(record.getValue().toString());
				
				RecordFx r = new RecordFx(record.getKey().toString(), values.toString().substring(1, values.toString().length()-1));
				group_values.add(new Pair<String, String>(r.getKey().toString(), r.getValue().toString()));
				
				output.addRecord(r);
				addLink(record, r);
				
				nodeTransition.getChildren().add(r.getFadeIn());
			} else { // record key found			
				
				RecordFx prevRecord = output.getRecords().get(index);
				addLink(record, prevRecord);
				
				values = groups.get(record.getKey().toString());
				values.add(record.getValue().toString());
				group_values.set(index, new Pair<>(record.getKey().toString(), values.toString().substring(1, values.toString().length()-1)));
				
				nodeTransition.getChildren().add(textUpdate(prevRecord, null, group_values.get(index).getValue()));
			}
		}
		
		systemTransition.getChildren().add(nodeTransition);
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}

	@Override
	public Transition count(boolean byKey) {
		ParallelTransition systemTransition = new ParallelTransition(search());
		SequentialTransition nodeTransition;
		
		String overallCount = "0";
		
		nodeTransition = new SequentialTransition();
		
		ArrayList<Pair<String, String>> count_values = new ArrayList<>();
		
		RDDPartitionFx input = structs.get(structs.size()-1);
		
		addStruct(new RDDPartitionFx()); // destination struct
		
		RDDPartitionFx output = structs.get(structs.size()-1);

		for (RecordFx record : input.getRecords()) {
			int index = byKey ? indexByKeyOf(record, output.getRecords()) : 0;
			
			if (byKey && index == -1) // new record key
			{	
				RecordFx r = new RecordFx(record.getKey().toString(), "1");
				count_values.add(new Pair<String, String>(r.getKey().toString(), r.getValue().toString()));
				
				output.addRecord(r);
				addLink(record, r);
				
				nodeTransition.getChildren().add(r.getFadeIn());
			} else { // record key found			
				if (output.getRecords().isEmpty()) {
					RecordFx r = new RecordFx(null, "1");
					
					count_values.add(new Pair<String, String>(null, "1"));
					output.addRecord(r);
					if (byKey) addLink(record, r);
					nodeTransition.getChildren().add(r.getFadeIn());
					overallCount = Integer.parseInt(overallCount) + 1 + "";
					continue;
				}
				
				RecordFx prevRecord = output.getRecords().get(index);
				if (byKey) addLink(record, prevRecord);
				
				count_values.set(index, new Pair<>(record.getKey().toString(), Integer.parseInt(count_values.get(index).getValue()) + 1 + ""));
				
				overallCount = Integer.parseInt(overallCount) + 1 + "";
				
				nodeTransition.getChildren().add(textUpdate(prevRecord, null, count_values.get(index).getValue()));
			}
		}
		
		systemTransition.getChildren().add(nodeTransition);
		
		final String final_overallCount = overallCount;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The overall count is: " + final_overallCount));
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}

	@Override
	public Transition min(boolean byKey, boolean onKey) {
		ParallelTransition systemTransition = new ParallelTransition();
		systemTransition.getChildren().add(search());
		SequentialTransition nodeTransition;
		
		String overallMin = null;
		RecordFx overallMinRecord = null;
		
		nodeTransition = new SequentialTransition();
		
		RDDPartitionFx input = structs.get(structs.size()-1);
		
		addStruct(new RDDPartitionFx()); // destination struct
		
		RDDPartitionFx output = structs.get(structs.size()-1);
		
		ArrayList<Pair<String, String>> min_values = new ArrayList<>();	

		for (RecordFx record : input.getRecords()) {
			int index = byKey ? indexByKeyOf(record, output.getRecords()) : 0;
			
			if (byKey && index == -1) // new record key
			{	
				min_values.add(new Pair<>(record.getKey().toString(), record.getValue().toString()));
				
				RecordFx r = record.copy();
				output.addRecord(r);
				addLink(record, r);
				
				nodeTransition.getChildren().add(r.getFadeIn());
			} else { // record key found
				String element1 = onKey ? record.getKey().toString() : record.getValue().toString();
				
				// if toRDD is empty it is an aggregate not by key
				if (output.getRecords().isEmpty()) {
					min_values.add(new Pair<>(record.getKey().toString(), record.getValue().toString()));
					RecordFx r = record.copy();
					output.addRecord(r);
					if (byKey) addLink(record, r);
					nodeTransition.getChildren().add(r.getFadeIn());
					if (overallMin == null || 
							Double.parseDouble(overallMin) > Double.parseDouble(element1)) {
						overallMin = element1;
						overallMinRecord = record;
					}
					
					continue;
				}
				
				RecordFx prevRecord = output.getRecords().get(index);
				if (byKey) addLink(record, prevRecord);
				
				String element2 = onKey ? min_values.get(index).getKey() : min_values.get(index).getValue();
				
				if (Double.parseDouble(element2) > Double.parseDouble(element1)) {
					min_values.set(index, new Pair<>(record.getKey().toString(), record.getValue().toString()));
				}
				
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
		
		final String final_overallMin = overallMin;
		final RecordFx final_overallMinRecord = overallMinRecord;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The minimum value is: " + final_overallMin + "\nrecord: "+final_overallMinRecord));
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}

	@Override
	public Transition max(boolean byKey, boolean onKey) {
		ParallelTransition systemTransition = new ParallelTransition();
		systemTransition.getChildren().add(search());
		SequentialTransition nodeTransition;
		
		RecordFx overallMaxRecord = null;
		String overallMax = null;
		
		RDDPartitionFx input = structs.get(structs.size()-1);
		
		addStruct(new RDDPartitionFx()); // destination struct
		
		RDDPartitionFx output = structs.get(structs.size()-1);
		
		nodeTransition = new SequentialTransition();
		
		ArrayList<Pair<String, String>> max_values = new ArrayList<>();

		for (RecordFx record : input.getRecords()) {
			int index = byKey ? indexByKeyOf(record, output.getRecords()) : 0;
			String element1 = onKey ? record.getKey().toString() : record.getValue().toString();
			
			if (byKey && index == -1) // new record key
			{	
				max_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
				
				RecordFx r = record.copy();
				output.addRecord(r);
				addLink(record, r);
				nodeTransition.getChildren().add(r.getFadeIn());
			} else { // record key found
				if (output.getRecords().isEmpty()) {
					max_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
					RecordFx r = record.copy();
					output.addRecord(r);
					if (byKey) addLink(record, r);
					nodeTransition.getChildren().add(r.getFadeIn());
					if (overallMax == null || 
							Double.parseDouble(overallMax) < Double.parseDouble(element1)) {
						overallMax = element1;
						overallMaxRecord = record;
					}
						
					continue;
				}
				
				RecordFx prevRecord = output.getRecords().get(index);
				if (byKey) addLink(record, prevRecord);
				
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
		
		
		final String final_overallMax = overallMax;
		final RecordFx final_overallMaxRecord = overallMaxRecord;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The maximum value is: " + final_overallMax + "\nrecord: " + final_overallMaxRecord));
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}

	@Override
	public Transition sum(boolean byKey, boolean onKey, boolean isInteger) {
		ParallelTransition systemTransition = new ParallelTransition(search());
		SequentialTransition nodeTransition;
		
		String overallSum = "0";
		
		RDDPartitionFx input = structs.get(structs.size()-1);
		
		addStruct(new RDDPartitionFx()); // destination struct
		
		RDDPartitionFx output = structs.get(structs.size()-1);
		
		nodeTransition = new SequentialTransition();
		
		ArrayList<Pair<String, String>> sum_values = new ArrayList<>();

		for (RecordFx record : input.getRecords()) {
			int index = byKey ? indexByKeyOf(record, output.getRecords()) : 0;
			
			if (byKey && index == -1) // new record key
			{	
				sum_values.add(new Pair<String, String>(record.getKey().toString(), record.getValue().toString()));
				
				RecordFx r = record.copy();
				output.addRecord(r);
				addLink(record, r);
				nodeTransition.getChildren().add(r.getFadeIn());
			} else { // record key found	
				String element = onKey ? record.getKey().toString() : record.getValue().toString();
				
				if (output.getRecords().isEmpty()) {
					RecordFx r = new RecordFx(null, element);
					
					sum_values.add(new Pair<String, String>(null, r.getValue().toString()));
					output.addRecord(r);
					if (byKey) addLink(record, r);
					nodeTransition.getChildren().add(r.getFadeIn());
					overallSum = isInteger ? Integer.parseInt(overallSum) + 
								Integer.parseInt(element) + "" : 
								Double.parseDouble(overallSum) + 
								Double.parseDouble(element) + "";
					
					continue;
				}
				
				RecordFx prevRecord = output.getRecords().get(index);
				
				if (byKey) addLink(record, prevRecord);
				
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
		
		final String final_overallSum = overallSum;
		
		if (!byKey) systemTransition.setOnFinished( event -> information("The overall sum is: " + final_overallSum));
		
		setCurrentTransition(systemTransition);
		
		return systemTransition;
	}
	
	public Transition search() {
		SequentialTransition systemTransition = new SequentialTransition();

		for (RecordFx record : structs.get(structs.size()-1).getRecords())
			systemTransition.getChildren().add(record.getColorChange());

		return systemTransition;
	}

	@Override
	public Transition reduceByKey(String operation, Button done_button) {
		Transition t = operationToTransition(operation, true);
		t.setOnFinished(event -> done_button.setDisable(false));
		
		return t;
	}
	
	public void setTitle(String struct_title) {
		structs.get(structs.size()-1).setTitle(struct_title);
	}
}

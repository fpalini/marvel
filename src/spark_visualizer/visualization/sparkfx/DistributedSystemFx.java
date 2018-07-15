package spark_visualizer.visualization.sparkfx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javafx.animation.*;
import javafx.scene.Group;
import javafx.scene.control.Alert;
import javafx.scene.control.Label;
import javafx.util.Duration;
import scala.Tuple2;

public class DistributedSystemFx extends Group {

	public final static int MARGIN = 30;
	public final static int PADDING = 50;

	public final static int RDD_FROM = 0;
	public final static int RDD_TO = 1;

	private ArrayList<ExecutorFx> executors = new ArrayList<>();
	private ArrayList<Label> labels = new ArrayList<>();

	private int nExecutors;
	private int blockSize;

	private double height, width;

	private ParallelTransition parallelTransition = new ParallelTransition();
	private ParallelTransition searchTransition = new ParallelTransition();

	public DistributedSystemFx(int nExecutors, int blockSize) {
		this.nExecutors = nExecutors;
		this.blockSize = blockSize;

		ExecutorFx executor;
		Label label;

		for (int i = 0; i < nExecutors; i++) {
			executor = new ExecutorFx(0, 0);
			executors.add(executor);

			label = new Label("Executor #" + (i+1));
			labels.add(label);
		}

		relocate();

		getChildren().addAll(executors);
		getChildren().addAll(labels);
	}

	public void parallelize(List<Tuple2<String,String>> dataset) {
		createRDD(dataset, executors, blockSize);
		relocate();

		addSearchAnimation();
	}
	
	public void parallelize(List<Tuple2<String,String>> datasetPartitions[]) {
		createRDD(datasetPartitions, blockSize);
		relocate();

		addSearchAnimation();
	}

	public void createRDD(List<Tuple2<String, String>>[] datasetPartitions, int blockSize2) {
		
		for (int e = 0; e < nExecutors; e++) {
			Iterator<Tuple2<String, String>> data_iterator = datasetPartitions[e].iterator();
			ArrayList<BlockFx> blocks = new ArrayList<>();
	
			int recordCounter;
			BlockFx block;
			RecordFx record;
	
			while (data_iterator.hasNext()) {
				recordCounter = 0;
				block = new BlockFx();
	
				while (data_iterator.hasNext() && recordCounter < blockSize) {
					Tuple2<String, String> keyValue = data_iterator.next();
	
					record = new RecordFx(keyValue._1, keyValue._2);
					record.getFadeIn().play();
					block.addRecord(record);
					recordCounter++;
				}
	
				blocks.add(block);
			}
	
			Iterator<BlockFx> block_iterator = blocks.iterator();
	
			while (block_iterator.hasNext())
				executors.get(e).addBlockFromRDD(block_iterator.next());
		}
	}

	public void relocate() {
		int cols = nExecutors < 6 ? nExecutors : 6;

		double maxHeight = 0;

		for (ExecutorFx executor : executors)
			if (maxHeight < executor.height())
				maxHeight = executor.height();


		int label_padding = 20;

		ExecutorFx executor;
		Label label;
		int r, c;
		double x, y;

		for (int i = 0; i < nExecutors; i++) {
			r = i / cols;
			c = i % cols;

			x = MARGIN + c * (ExecutorFx.WIDTH + PADDING);
			y = MARGIN + r * (maxHeight + PADDING);

			executor = executors.get(i);
			label = labels.get(i);

			executor.setLayoutX(x);
			executor.setLayoutY(y);

			label.setLayoutX(x);
			label.setLayoutY(y - label_padding);
		}
	}

	public DistributedSystemFx copy() {
		DistributedSystemFx system = new DistributedSystemFx(nExecutors, blockSize);
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

	public void createRDD(List<Tuple2<String,String>> dataset, ArrayList<ExecutorFx> executors, int blockSize) {

		Iterator<Tuple2<String, String>> data_iterator = dataset.iterator();
		ArrayList<BlockFx> blocks = new ArrayList<>();

		int recordCounter;
		BlockFx block;
		RecordFx record;

		while (data_iterator.hasNext()) {
			recordCounter = 0;
			block = new BlockFx();

			while (data_iterator.hasNext() && recordCounter < blockSize) {
				Tuple2<String, String> keyValue = data_iterator.next();

				record = new RecordFx(keyValue._1, keyValue._2);
				record.getFadeIn().play();
				block.addRecord(record);
				recordCounter++;
			}

			blocks.add(block);
		}

		Iterator<BlockFx> block_iterator = blocks.iterator();

		while (block_iterator.hasNext())
			for (ExecutorFx executor : executors)
				if (block_iterator.hasNext())
					executor.addBlockFromRDD(block_iterator.next());
	}

	public void min() {
		BlockFx block;
		RecordFx first_record;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			first_record = executor.getFromRDD().getRecords().get(0);
			block = new BlockFx();
			block.addRecord(first_record.copy());
			executor.getToRDD().addBlock(block);
		}

		relocate(); //TODO

		String overallMin = null;
		String keyText, valueText;

		SequentialTransition minTransition;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			RecordFx minRecord = executor.getToRDD().getRecords().get(0);

			minTransition = new SequentialTransition();

			keyText =  minRecord.getKey() != null ? minRecord.getKey().toString() : null;
			valueText = minRecord.getValue().toString();

			for (RecordFx record : executor.getFromRDD().getRecords()) {
				if (Double.parseDouble(record.getValue().toString()) < Double.parseDouble(valueText)) {
					keyText =  record.getKey() != null ? record.getKey().toString() : null;
					valueText = record.getValue().toString();
				}

				minTransition.getChildren().add(textUpdate(minRecord, keyText, valueText));

				if (overallMin == null || 
						Double.parseDouble(record.getValue().toString()) < Double.parseDouble(overallMin)) {
					overallMin = record.getValue().toString();
				}
			}

			parallelTransition.getChildren().add(minTransition);
		}

		parallelTransition.play();

		String finalOverallMin = overallMin;
		parallelTransition.setOnFinished(event -> createAlert("The minimum value is: "+ finalOverallMin+"!"));
	}

	public void max() {
		BlockFx block;
		RecordFx first_record;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			first_record = executor.getFromRDD().getRecords().get(0);
			block = new BlockFx();
			block.addRecord(first_record.copy());
			executor.getToRDD().addBlock(block);
		}

		relocate(); //TODO

		String overallMax = null;
		String keyText, valueText;

		SequentialTransition maxTransition;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			RecordFx maxRecord = executor.getToRDD().getRecords().get(0);

			maxTransition = new SequentialTransition();

			keyText =  maxRecord.getKey() != null ? maxRecord.getKey().toString() : null;
			valueText = maxRecord.getValue().toString();

			for (RecordFx record : executor.getFromRDD().getRecords()) {
				if (Double.parseDouble(record.getValue().toString()) > Double.parseDouble(valueText)) {
					keyText =  record.getKey() != null ? record.getKey().toString() : null;
					valueText = record.getValue().toString();
				}

				maxTransition.getChildren().add(textUpdate(maxRecord, keyText, valueText));

				if (overallMax == null || 
						Double.parseDouble(record.getValue().toString()) > Double.parseDouble(overallMax)) {
					overallMax = record.getValue().toString();
				}
			}

			parallelTransition.getChildren().add(maxTransition);
		}

		parallelTransition.play();

		String finalOverallMax = overallMax;
		parallelTransition.setOnFinished(event -> createAlert("The maximum value is: "+ finalOverallMax+"!"));
	}

	public void count() {
		BlockFx block;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			block = new BlockFx();
			block.addRecord(new RecordFx(null,  "0"));
			executor.getToRDD().addBlock(block);
		}

		relocate(); //TODO

		Integer overallCount = 0;
		Integer count;

		SequentialTransition countTransition;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			RecordFx countRecord = executor.getToRDD().getRecords().get(0);

			countTransition = new SequentialTransition();

			count = 0;

			for (int i = 0; i < executor.getFromRDD().getNumRecords(); i++) {
				count += 1;
				countTransition.getChildren().add(textUpdate(countRecord, null, count.toString()));
			}
			
			overallCount += count;

			parallelTransition.getChildren().add(countTransition);
		}

		parallelTransition.play();

		String finalOverallCount = overallCount.toString();
		parallelTransition.setOnFinished(event -> createAlert("The number of values is: "+ finalOverallCount+"!"));
	}

	public void sum() {
		BlockFx block;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			block = new BlockFx();
			block.addRecord(new RecordFx(null,  "0.0"));
			executor.getToRDD().addBlock(block);
		}

		relocate(); //TODO

		Double overallSum = 0.0;
		Double sum;

		SequentialTransition sumTransition;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			RecordFx sumRecord = executor.getToRDD().getRecords().get(0);

			sumTransition = new SequentialTransition();

			sum = 0.0;

			for (RecordFx record : executor.getFromRDD().getRecords()) {
				sum += Double.parseDouble(record.getValue().toString());

				sumTransition.getChildren().add(textUpdate(sumRecord, null, sum.toString()));
			}
			
			overallSum += sum;

			parallelTransition.getChildren().add(sumTransition);
		}

		parallelTransition.play();

		String finalOverallSum = overallSum.toString();
		parallelTransition.setOnFinished(event -> createAlert("The sum of the values is: "+ finalOverallSum+"!"));
	}

	public void filter(String condition, String value) {
		relocate();

		ParallelTransition filterTransition;

		BlockFx block;

		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			block = new BlockFx();
			boolean toAdd = true;

			filterTransition = new ParallelTransition();

			int delay = 0;

			for (RecordFx record : executor.getFromRDD().getRecords()) {
				if (filterEval(condition, record.getValue().toString(), value))
				{
					toAdd = true;

					if (block.getRecords().size() == blockSize) {
						executor.getToRDD().addBlock(block);
						toAdd = false;
						block = new BlockFx();
					}

					RecordFx r = record.copy();

					block.addRecord(r);

					r.getFadeIn().setDelay(Duration.millis(delay * 2 * FieldFx.ANIMATION_MS));
					filterTransition.getChildren().add(r.getFadeIn());
				}
				delay++;
			}

			if (toAdd && block.size() > 0) executor.getToRDD().addBlock(block);

			parallelTransition.getChildren().add(filterTransition);
		}

		parallelTransition.play();
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
			return Integer.parseInt(value1) == Integer.parseInt(value2);
		case "!=":
			if (value1.equals("null"))
				return true;
			return Integer.parseInt(value1) != Integer.parseInt(value2);
		case "not null":
			return !value1.equals("null");
		default:
			return false;
		}
	}

	private Timeline textUpdate(RecordFx record, String keyText, String valueText) {
		Timeline timeline = new Timeline();

		if (keyText != null) {
			KeyValue kv1 = new KeyValue(record.getKey().getLabel().textProperty(), keyText);
			KeyFrame kf1 = new KeyFrame(Duration.millis(2 * FieldFx.ANIMATION_MS), kv1);

			timeline.getKeyFrames().add(kf1);
		}

		KeyValue kv2 = new KeyValue(record.getValue().getLabel().textProperty(), valueText);
		KeyFrame kf2 = new KeyFrame(Duration.millis(2 * FieldFx.ANIMATION_MS), kv2);

		timeline.getKeyFrames().add(kf2);

		return timeline;
	}

	public void overwriteFromRDD() {
		for (ExecutorFx executor : executors) {
			executor.getFromRDD().clear();
			
			for (BlockFx block : executor.getToRDD().getBlocks())
				executor.getFromRDD().addBlock(block.copy());
			
			executor.getToRDD().clear();
		}
		
		addSearchAnimation();
	}

	private void addSearchAnimation() {
		for (ExecutorFx executor : executors) {
			if (executor.getFromRDD().isEmpty()) continue;
			
			SequentialTransition searchTransitionInternal = new SequentialTransition();

			for (RecordFx record : executor.getFromRDD().getRecords())
				searchTransitionInternal.getChildren().add(record.getColorChange());

			searchTransition.getChildren().add(searchTransitionInternal);
		}

		parallelTransition.getChildren().add(searchTransition);
	}

	private void createAlert(String message) {
		Alert alert = new Alert(Alert.AlertType.INFORMATION);
		alert.setTitle("Computation Result");
		alert.setHeaderText(null);
		alert.setContentText(message);

		alert.show();
	}

	public ArrayList<ExecutorFx> getExecutors() { return executors; }

	public double height() { return height; }

	public double width() { return width; }

	public void setHeight(double height) { this.height = height; }

	public void setWidth(double width) { this.width = width; }
}

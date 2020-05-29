package marvel.visualization.sparkfx;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import javafx.animation.KeyFrame;
import javafx.animation.KeyValue;
import javafx.animation.PauseTransition;
import javafx.animation.SequentialTransition;
import javafx.animation.Timeline;
import javafx.animation.Transition;
import javafx.geometry.Bounds;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.layout.Pane;
import javafx.scene.shape.LineTo;
import javafx.util.Duration;
import scala.Tuple2;

public abstract class SystemFx extends Pane {
	
	private String system_name;
	private Transition currentTransition;
	
	private double speed = 1.0;

	abstract public void createInput(List<Tuple2<String, String>> dataset);

	
	public void setSystemName(String name) { system_name = name; }


	abstract public SystemFx copy();
	
	public double getSpeed() {
		return speed;
	}
	
	@Override
	public String toString() {
		return system_name;
	}

	public boolean filterEval(String condition, String value1, String value2) {
		Double value;

		switch (condition) {
		case ">":
			value = value1.equals("null") ? Double.MIN_VALUE : Double.parseDouble(value1);
			return value > Integer.parseInt(value2);
		case "<":
			value = value1.equals("null") ? Double.MAX_VALUE : Double.parseDouble(value1);
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

	public int indexByKeyOf(RecordFx record, ArrayList<RecordFx> records) {
		int index = -1;
		
		for (int r = 0; r < records.size(); r++)
			if (record.getKey().equals(records.get(r).getKey()))
				index = r;
		
		return index;
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
	
	public Transition textUpdate(RecordFx record, String keyText, String valueText) {
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

	public Transition getCurrentTransition() {
		// TODO Auto-generated method stub
		return currentTransition;
	}
	
	public void setCurrentTransition(Transition t) {
		currentTransition = t;
	}

	public void setRate(double speed_value) {
		speed = speed_value;
		getCurrentTransition().setRate(speed);
	}
	
	private Button done_button;

	public void setDoneButton(Button done_button) {
		this.done_button = done_button;
	}
	
	public Button getDoneButton() {
		return done_button;
	}
	
	public Transition operationToTransition(String operation, boolean isFirst) {
		switch (operation) {
			case "ReduceByKey + Count": return isFirst ? count(true) : sum(true, false, true);
			case "ReduceByKey + Min": return min(true, false);
			case "ReduceByKey + Max": return max(true, false);
			case "ReduceByKey + Sum": return sum(true, false, false);
			case "GroupByKey": return groupByKey();
		}
		
		return null;
	}

	abstract public Transition swap();


	abstract public Transition filter(String condition, String value, boolean b);


	abstract public Transition split();


	abstract public Transition count(boolean b);


	abstract public Transition min(boolean b, boolean c);


	abstract public Transition max(boolean b, boolean c);


	abstract public Transition sum(boolean b, boolean c, boolean d);
	
	abstract public Transition search();

	abstract public Transition reduceByKey(String operation, Button done_button);
	
	abstract public Transition groupByKey();
	
	public Bounds getSystemBounds(Node node) {
		Parent parent = node.getParent();
		Bounds bounds = node.getBoundsInParent();
		
		while (!parent.getClass().equals(this.getClass())) {
			bounds = parent.localToParent(bounds);
			parent = parent.getParent();
		}
		
		return bounds;
	}

	public TreeSet<String> stringToOrderedSet(String stringArray) {
		TreeSet<String> strings = new TreeSet<>();
		
		for (String s : stringArray.split(", "))
		      strings.add(s);
		
		return strings;
	}


	abstract public Transition flatMapToPair();

}

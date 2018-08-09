package spark_visualizer.visualization;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;

import javafx.animation.Transition;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Insets;
import javafx.scene.Group;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.stage.FileChooser;
import javafx.stage.FileChooser.ExtensionFilter;
import javafx.stage.Stage;
import javafx.util.Pair;
import scala.Tuple2;
import spark_visualizer.orchestrator.Orchestrator;
import spark_visualizer.visualization.sparkfx.DistributedSystemFx;
import spark_visualizer.visualization.sparkfx.NodeFx;
import spark_visualizer.visualization.sparkfx.RecordFx;

import org.controlsfx.control.BreadCrumbBar;

public class SparkVisualizerController implements Initializable {

	@FXML
	private Group canvas;

	@FXML
	private ChoiceBox<String> keytype, valuetype;

	@FXML
	private ComboBox<String> map_list, reduce_list;

	@FXML
	private Slider zoom_slider, speed_slider;

	@FXML
	private Label zoom_label, speed_label;
	
	@FXML
	private Button done_button, run_button;

	@FXML
	private ScrollPane scrollpane;

	@FXML
	private BreadCrumbBar<DistributedSystemFx> stages;

	@FXML
	private TextField split, keycol, valuecol;

	@FXML
	private TextField datasize, blocksize, nodes;

	private File input_file;

	private DistributedSystemFx currentSystem;

	private Orchestrator orchestrator;

	private double zoom_value = 1;
	private double speed_value = 1;

	private TreeItem<DistributedSystemFx> selectedStage;

	private String map_operations[] = new String[] { "Swap", "FilterOnKey", "FilterOnValue" };
	private String reduce_operations[] = new String[] { "Count", "MinOnKey", "MinOnValue", 
			"MaxOnKey", "MaxOnValue", "SumOnKey", "SumOnValue", "ReduceByKey + Count", "ReduceByKey + Min", "ReduceByKey + Max", "ReduceByKey + Sum" };
	private String operation = reduce_operations[0];

	/**
	 * Resets the system, generates a new random dataset
	 * and partitions it among the nodes.
	 */
	@FXML
	private void generate() {
		reset();

		List<Tuple2<String,String>> dataset = createDataset();

		currentSystem.parallelize(dataset);

		currentSystem.setSystemName("Generate");
		stages.setSelectedCrumb(new TreeItem<>(currentSystem));
		selectedStage = stages.getSelectedCrumb();
		
		for (NodeFx node : currentSystem.getNodes())
			if (node.getFromRDD().size() == 0)
        		node.setColor(Color.CRIMSON);
        	else
        		node.setColor(Color.DEEPSKYBLUE);

		setCurrentSystem(currentSystem.copy());
	}

	/**
	 * Generates a random dataset.
	 * 
	 * @return list of {@literal <key, value>} pairs, representing the dataset.
	 */
	private List<Tuple2<String,String>> createDataset() {

		if (input_file != null)
			orchestrator.createRDDfromFile(Integer.parseInt(datasize.getText()), input_file, 
					split.getText().trim(), Integer.parseInt(keycol.getText()), 
					Integer.parseInt(valuecol.getText()));
		else
			orchestrator.createRandomRDD(keytype.getValue(), valuetype.getValue(), 
					Integer.parseInt(datasize.getText()));

		return orchestrator.getDataset();
	}

	/**
	 * Computes the map or reduce function selected from the lists.
	 * 
	 * @throws IOException caused by the filter options window.
	 */
	@FXML
	void run() throws IOException {
		String map_function = map_list.getValue();
		String reduce_function = reduce_list.getValue();

		// execution of both map and reduce function not permitted.
		if (!map_function.equals("-") && !reduce_function.equals("-"))
			return;

		// execution with more than one RDD not permitted.
		for (NodeFx node : currentSystem.getNodes())
			if (node.getToRDD().size() > 0)
				node.getToRDD().clear();

		// Execution after an aggregation (not by key) not permitted
		for (String op : Arrays.copyOf(reduce_operations, reduce_operations.length-1))
			if (selectedStage.getValue().toString().equals(op)) {
				DistributedSystemFx.warning("End of Execution!");
				return;
			}

		switch (map_function) {
		case "Swap":
			currentSystem.swap();
			currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("Swap");
			break;
		case "FilterOnKey":
			if (openFilterOptions()) {
				currentSystem.filter(condition, value, true);
				currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
				currentSystem.getCurrentTransition().setRate(speed_value);
				currentSystem.getCurrentTransition().play();
				currentSystem.setSystemName("FilterOnKey");
			}
			break;
		case "FilterOnValue":
		if (openFilterOptions()) {
			currentSystem.filter(condition, value, false);
			currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("FilterOnValue");
		}
		break;
	}

		switch (reduce_function) {
		case "Count":
			currentSystem.count(false);
			currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("Count");
			break;
		
		case "MinOnKey":
			for (NodeFx node : currentSystem.getNodes()) {
				try {
					for (RecordFx record : node.getFromRDD().getRecords()) 
						Double.parseDouble(record.getKey().toString());
				} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
			}
			currentSystem.min(false, true);
			currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("MinOnKey");
			break;
		case "MinOnValue":
			for (NodeFx node : currentSystem.getNodes()) {
				try {
					for (RecordFx record : node.getFromRDD().getRecords()) 
						Double.parseDouble(record.getValue().toString());
				} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
			}
			currentSystem.min(false, false);
			currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("MinOnValue");
			break;
		
		case "MaxOnKey":
			for (NodeFx node : currentSystem.getNodes()) {
				try {
					for (RecordFx record : node.getFromRDD().getRecords()) 
						Double.parseDouble(record.getKey().toString());
				} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
			}
			currentSystem.max(false, true);
			currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("MaxOnKey");
			break;
		case "MaxOnValue":
			for (NodeFx node : currentSystem.getNodes()) {
				try {
					for (RecordFx record : node.getFromRDD().getRecords()) 
						Double.parseDouble(record.getValue().toString());
				} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
			}
			currentSystem.max(false, false);
			currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("MaxOnValue");
			break;
		
		case "SumOnKey":
			for (NodeFx node : currentSystem.getNodes()) {
				try {
					for (RecordFx record : node.getFromRDD().getRecords()) 
						Double.parseDouble(record.getKey().toString());
				} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
			}
			currentSystem.sum(false, true);
			currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("SumOnKey");
			break;
		case "SumOnValue":
			for (NodeFx node : currentSystem.getNodes()) {
				try {
					for (RecordFx record : node.getFromRDD().getRecords()) 
						Double.parseDouble(record.getValue().toString());
				} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
			}
			currentSystem.sum(false, false);
			currentSystem.getCurrentTransition().setOnFinished(event -> done_button.setDisable(false));
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("SumOnValue");
			break;
			
		case "ReduceByKey + Count":
			currentSystem.reduceByKey(operation, done_button);
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("ReduceByKey + Count");
			break;
		
		
		case "ReduceByKey + Min":
			currentSystem.reduceByKey(operation, done_button);
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("ReduceByKey + Min");
			break;
		
		case "ReduceByKey + Max":
			currentSystem.reduceByKey(operation, done_button);
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("ReduceByKey + Max");
			break;
			
		case "ReduceByKey + Sum":
			currentSystem.reduceByKey(operation, done_button);
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("ReduceByKey + Sum");
			break;
		}
		
	}


	/**
	 * Creates a snapshot of the system, increases the phase
	 * and the result RDD becomes the working RDD of the new phase.
	 */
	@FXML
	void done() {
		int numVoid = 0;
		for (NodeFx node : currentSystem.getNodes())
			if (node.getToRDD().size() == 0) numVoid++;

		// if all the nodes 
		if (numVoid == Integer.parseInt(nodes.getText())) return;

		currentSystem.overwriteFromRDD();

		TreeItem<DistributedSystemFx> crumb_currentSystem = new TreeItem<>(currentSystem);
		selectedStage.getChildren().clear();
		selectedStage.getChildren().add(crumb_currentSystem);
		stages.setSelectedCrumb(crumb_currentSystem);
		selectedStage = crumb_currentSystem;
		
		// if (crumb_currentSystem.toString().startsWith("ReduceByKey")) crumb_currentSystem.getGraphic().setStyle(value);

		for (NodeFx node : currentSystem.getNodes())
			if (node.getFromRDD().size() == 0)
        		node.setColor(Color.CRIMSON);
        	else
        		node.setColor(Color.DEEPSKYBLUE);
		
		setCurrentSystem(currentSystem.copy());

		map_list.setValue("-");
		reduce_list.setValue("-");
		
		done_button.setDisable(true);
	}

	private String condition = ">", value = "0";

	/**
	 * Opens a window with the filter option. It should be called 
	 * only when the filter option is selected.
	 */
	private boolean openFilterOptions() throws IOException {
		// Create the custom dialog.
		Dialog<String[]> dialog = new Dialog<>();
		dialog.setTitle("Filter Options");

		dialog.getDialogPane().getButtonTypes().addAll(ButtonType.APPLY, ButtonType.CANCEL);

		GridPane grid = new GridPane();
		grid.setHgap(10);
		grid.setVgap(10);
		grid.setPadding(new Insets(20, 150, 10, 10));

		TextField value = new TextField();
		ChoiceBox<String> condition = new ChoiceBox<>();

		condition.getItems().add(">");
		condition.getItems().add("<");
		condition.getItems().add("=");
		condition.getItems().add("!=");

		condition.setValue(">");
		value.setText("0");

		grid.add(new Label("Condition:"), 0, 0);
		grid.add(condition, 1, 0);
		grid.add(new Label("Value:"), 0, 1);
		grid.add(value, 1, 1);

		dialog.getDialogPane().setContent(grid);

		// Convert the result to a condition-value pair when the OK button is clicked.
		dialog.setResultConverter(
				dialogButton -> 
				dialogButton == ButtonType.APPLY ? 
						new String[] { condition.getValue(), value.getText() } : 
							null
				);

		Optional<String[]> result = dialog.showAndWait();

		result.ifPresent(
				conditionValue -> {
					this.condition = conditionValue[0];
					this.value = conditionValue[1];
				}
				);

		return result.isPresent();
	}

	/**
	 * Resets all the structures.
	 */
	public void reset() {		
		setCurrentSystem(new DistributedSystemFx(Integer.parseInt(nodes.getText()), 
				Integer.parseInt(blocksize.getText())));
	}

	@Override
	public void initialize(URL location, ResourceBundle resources) {
		// initialize map and reduce lists

		map_list.getItems().add("-");
		map_list.getItems().addAll(map_operations);

		reduce_list.getItems().add("-");
		reduce_list.getItems().addAll(reduce_operations);
		
		map_list.setValue("-");
		reduce_list.setValue("-");

		// initialize key and value type

		keytype.getItems().add("String");
		keytype.getItems().add("Integer");
		keytype.getItems().add("Double");
		keytype.getItems().add("-");

		valuetype.getItems().add("String");
		valuetype.getItems().add("Integer");
		valuetype.getItems().add("Double");
		
		done_button.setDisable(true);

		// initialize the zoom and speed sliders

		zoom_slider.valueProperty().addListener(
				(observable, oldValue, newValue) -> zoom(newValue.doubleValue() / 100)
				);
		
		speed_slider.valueProperty().addListener(
				(observable, oldValue, newValue) -> speed(newValue.doubleValue() / 100)
				);

		// initialize bread crumb bar

		stages.setAutoNavigationEnabled(false);

		stages.setOnCrumbAction(
				(event) -> {
					selectedStage = event.getSelectedCrumb();
					setCurrentSystem(selectedStage.getValue().copy());
					done_button.setDisable(true);
				}
				);
	}

	private void speed(double value) {
		speed_label.setText(String.format("%.1fx", value));
		
		speed_value = value;
		
		if (currentSystem.getCurrentTransition() != null) 
			currentSystem.getCurrentTransition().setRate(speed_value);
	}

	private void zoom(double value) {
		zoom_label.setText(String.format("%.1fx", value));

		zoom_value = value;

		currentSystem.setScaleX(zoom_value);
		currentSystem.setScaleY(zoom_value);
	}

	@FXML
	private void chooseFile() {
		FileChooser fileChooser = new FileChooser();
		fileChooser.getExtensionFilters().add(new ExtensionFilter("CSV",  "*.csv"));
		fileChooser.setTitle("Import Dataset File");
		input_file = fileChooser.showOpenDialog(new Stage());
	}

	private void setCurrentSystem(DistributedSystemFx ds) {
		currentSystem = ds;
		canvas.getChildren().set(0, currentSystem);
		zoom(zoom_value);
	}

	public void setDatasize(String d) {
		datasize.setText(d);
	}

	public void setBlocksize(String b) {
		blocksize.setText(b);
	}

	public void setNodes(String e) {
		nodes.setText(e);
	}

	public void setKeytype(String k) {
		keytype.setValue(k);
	}

	public void setValuetype(String v) {
		valuetype.setValue(v);
	}

	public void setFile(String path) {
		input_file = new File(path);
	}

	public void setSplit(String s) {
		split.setText(s);
	}

	public void setKeycol(String k) {
		keycol.setText(k);
	}

	public void setValuecol(String v) {
		valuecol.setText(v);
	}

	public void initSystem() {
		// initialize working system

		orchestrator = new Orchestrator(Integer.parseInt(nodes.getText()));

		currentSystem = new DistributedSystemFx(Integer.parseInt(nodes.getText()), 
				Integer.parseInt(blocksize.getText()));

		canvas.getChildren().add(currentSystem);
	}
}

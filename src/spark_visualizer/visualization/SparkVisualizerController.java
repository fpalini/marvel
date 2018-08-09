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
import javafx.stage.FileChooser;
import javafx.stage.FileChooser.ExtensionFilter;
import javafx.stage.Stage;
import javafx.util.Pair;
import scala.Tuple2;
import spark_visualizer.orchestrator.Orchestrator;
import spark_visualizer.visualization.sparkfx.DistributedSystemFx;
import spark_visualizer.visualization.sparkfx.ExecutorFx;

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
	private ScrollPane scrollpane;

	@FXML
	private BreadCrumbBar<DistributedSystemFx> stages;

	@FXML
	private TextField split, keycol, valuecol;

	@FXML
	private TextField datasize, blocksize, executors;

	private File input_file;

	private DistributedSystemFx currentSystem;

	private Orchestrator orchestrator;

	private double zoom_value = 1;
	private double speed_value = 1;

	private TreeItem<DistributedSystemFx> selectedStage;

	private String map_operations[] = new String[] { "Swap", "Filter" };
	private String reduce_operations[] = new String[] { "Count", "Min", "Max", "Sum", "ReduceByKey" };
	private String operation = reduce_operations[0];

	/**
	 * Resets the system, generates a new random dataset
	 * and partitions it among the executors.
	 */
	@FXML
	private void generate() {
		reset();

		List<Tuple2<String,String>> dataset = createDataset();

		currentSystem.parallelize(dataset);

		currentSystem.setSystemName("Generate");
		stages.setSelectedCrumb(new TreeItem<>(currentSystem));
		selectedStage = stages.getSelectedCrumb();

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
	void compute() throws IOException {
		String map_function = map_list.getValue();
		String reduce_function = reduce_list.getValue();

		// execution of both map and reduce function not permitted.
		if (!map_function.equals("-") && !reduce_function.equals("-"))
			return;

		// execution with more than one RDD not permitted.
		for (ExecutorFx executor : currentSystem.getExecutors())
			if (executor.getToRDD().size() > 0)
				executor.getToRDD().clear();

		// Execution after an aggregation (not by key) not permitted
		for (String op : Arrays.copyOf(reduce_operations, reduce_operations.length-1))
			if (selectedStage.getValue().toString().equals(op)) {
				DistributedSystemFx.warning("End of Execution!");
				return;
			}

		switch (map_function) {
		case "Swap":
			currentSystem.swap();
			currentSystem.getCurrentTransition().play();
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.setSystemName("Swap");
			break;
		case "Filter":
			if (openFilterOptions()) {
				currentSystem.filter(condition, value);
				currentSystem.getCurrentTransition().play();
				currentSystem.getCurrentTransition().setRate(speed_value);
				currentSystem.setSystemName("Filter");
			}
			break;
		}

		switch (reduce_function) {
		case "Count":
			currentSystem.count(false);
			currentSystem.getCurrentTransition().play();
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.setSystemName("Count");
			break;
		case "Min":
			currentSystem.min(false);
			currentSystem.getCurrentTransition().play();
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.setSystemName("Min");
			break;
		case "Max":
			currentSystem.max(false);
			currentSystem.getCurrentTransition().play();
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.setSystemName("Max");
			break;
		case "Sum":
			currentSystem.sum(false);
			currentSystem.getCurrentTransition().play();
			currentSystem.getCurrentTransition().setRate(speed_value);
			currentSystem.setSystemName("Sum");
			break;
		case "ReduceByKey":
			if (openReduceByKeyOptions()) {
				currentSystem.reduceByKey(operation);
				currentSystem.getCurrentTransition().play();
				currentSystem.getCurrentTransition().setRate(speed_value);
				currentSystem.setSystemName("ReduceByKey");
			}
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
		for (ExecutorFx executor : currentSystem.getExecutors())
			if (executor.getToRDD().size() == 0) numVoid++;

		// if all the executors 
		if (numVoid == Integer.parseInt(executors.getText())) return;

		currentSystem.overwriteFromRDD();

		TreeItem<DistributedSystemFx> crumb_currentSystem = new TreeItem<>(currentSystem);
		selectedStage.getChildren().clear();
		selectedStage.getChildren().add(crumb_currentSystem);
		stages.setSelectedCrumb(crumb_currentSystem);
		selectedStage = crumb_currentSystem;

		setCurrentSystem(currentSystem.copy());

		map_list.setValue("-");
		reduce_list.setValue("-");
	}

	private String condition = ">", value = "0";

	/**
	 * Opens a window with the filter option. It should be called 
	 * only when the filter option is selected.
	 */
	private boolean openFilterOptions() throws IOException {
		// Create the custom dialog.
		Dialog<Pair<String, String>> dialog = new Dialog<>();
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
						new Pair<>(condition.getValue(), value.getText()) : 
							null
				);

		Optional<Pair<String, String>> result = dialog.showAndWait();

		result.ifPresent(
				conditionValue -> {
					this.condition = conditionValue.getKey();
					this.value = conditionValue.getValue();
				}
				);

		return result.isPresent();
	}

	private boolean openReduceByKeyOptions() {
		// Create the custom dialog.
		Dialog<Pair<String, String>> dialog = new Dialog<>();
		dialog.setTitle("ReduceByKey Options");

		dialog.getDialogPane().getButtonTypes().addAll(ButtonType.APPLY, ButtonType.CANCEL);

		GridPane grid = new GridPane();
		grid.setHgap(10);
		grid.setVgap(10);
		grid.setPadding(new Insets(20, 150, 10, 10));

		TextField value = new TextField();
		ComboBox<String> operations = new ComboBox<>();

		operations.getItems().addAll(Arrays.copyOf(reduce_operations, reduce_operations.length-1));

		operations.setValue(reduce_operations[0]);

		grid.add(new Label("Operation:"), 0, 0);
		grid.add(operations, 1, 0);

		dialog.getDialogPane().setContent(grid);

		// Convert the result to a condition-value pair when the OK button is clicked.
		dialog.setResultConverter(dialogButton -> dialogButton == ButtonType.APPLY ? 
				new Pair<>(operations.getValue(), value.getText()) : null);

		Optional<Pair<String, String>> result = dialog.showAndWait();

		result.ifPresent(operation -> this.operation = operations.getValue());

		return result.isPresent();
	}

	/**
	 * Resets all the structures.
	 */
	public void reset() {		
		setCurrentSystem(new DistributedSystemFx(Integer.parseInt(executors.getText()), 
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

	public void setExecutors(String e) {
		executors.setText(e);
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

		orchestrator = new Orchestrator(Integer.parseInt(executors.getText()));

		currentSystem = new DistributedSystemFx(Integer.parseInt(executors.getText()), 
				Integer.parseInt(blocksize.getText()));

		canvas.getChildren().add(currentSystem);
	}
}

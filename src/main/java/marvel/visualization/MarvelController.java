package marvel.visualization;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;

import javafx.animation.ParallelTransition;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Insets;
import javafx.scene.Group;
import javafx.scene.canvas.Canvas;
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.stage.FileChooser;
import javafx.stage.FileChooser.ExtensionFilter;
import marvel.orchestrator.Orchestrator;
import marvel.visualization.sparkfx.DistributedSystemFx;
import marvel.visualization.sparkfx.LocalSystemFx;
import marvel.visualization.sparkfx.NodeFx;
import marvel.visualization.sparkfx.RDDPartitionFx;
import marvel.visualization.sparkfx.RecordFx;
import marvel.visualization.sparkfx.SystemFx;
import javafx.stage.Stage;
import scala.Tuple2;

import org.controlsfx.control.BreadCrumbBar;

import impl.org.controlsfx.skin.BreadCrumbBarSkin.BreadCrumbButton;

public class MarvelController implements Initializable {

	@FXML
	private AnchorPane canvas;

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
	private BreadCrumbBar<SystemFx> stages;

	@FXML
	private TextField split, keycol, valuecol;

	@FXML
	private TextField datasize, blocksize, nodes;
	
	private int rowsize;

	private File input_file;

	private SystemFx currentSystem;

	private Orchestrator orchestrator;
	
	private boolean local_enabled;

	private double zoom_value = 1;
	private double speed_value = 1;

	private TreeItem<SystemFx> selectedStage;

	private String map_operations[] = new String[] { "Swap", "FilterOnKey", "FilterOnValue", "Split", "FlatMapToPair"};
	private String reduce_operations[] = new String[] { "Count", "MinOnKey", "MinOnValue", 
			"MaxOnKey", "MaxOnValue", "SumOnKey", "SumOnValue", "GroupByKey", "ReduceByKey + Count", "ReduceByKey + Min", "ReduceByKey + Max", "ReduceByKey + Sum" };

	@FXML
	private TabPane tabPane;

	/**
	 * Resets the system, generates a new random dataset
	 * and partitions it among the nodes.
	 */
	@FXML
	private void generate() {
		reset();
		
		tabPane.getSelectionModel().select(1);

		List<Tuple2<String,String>> dataset = createDataset();

		currentSystem.createInput(dataset);

		currentSystem.setSystemName("Generate");
		if (currentSystem instanceof LocalSystemFx) ((LocalSystemFx) currentSystem).setTitle("Generate");
		stages.setSelectedCrumb(new TreeItem<>(currentSystem));
		selectedStage = stages.getSelectedCrumb();
		
		if (!local_enabled)
			for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes())
				if (node.getFromRDD().size() == 0)
	        		node.setColor(NodeFx.RED);
	        	else
	        		node.setColor(NodeFx.GREEN);

		setCurrentSystem(currentSystem.copy());
		
		initTransition();
		
		run_button.setDisable(false);
	}

	private void initTransition() {
		ParallelTransition initTransition = new ParallelTransition();
		
		if (!local_enabled) {
			for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes())
				for (RecordFx record : node.getFromRDD().getRecords())
					initTransition.getChildren().add(record.getFadeIn());	
			}
		else {
			for(RDDPartitionFx rdd : ((LocalSystemFx) currentSystem).getStructs())
				for (RecordFx record : rdd.getRecords())
					initTransition.getChildren().add(record.getFadeIn());	
		}
			
		initTransition.play();
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
		if (map_function.equals("-") == reduce_function.equals("-"))
			return;
		
		run_button.setDisable(true);

		// execution with more than one RDD not permitted.
		if (!local_enabled)
			for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes())
				if (node.getToRDD().size() > 0)
					node.getToRDD().clear();

		// Execution after an aggregation (not by key) not permitted
		for (String op : reduce_operations)
			if (!op.contains("ByKey") && selectedStage.getValue().toString().equals(op)) {
				DistributedSystemFx.warning("End of Execution!");
				return;
			}
		
		if (currentSystem.getDoneButton() == null) currentSystem.setDoneButton(done_button);

		switch (map_function) {
		case "Swap":
			currentSystem.swap();
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("Swap");
			break;
		case "FilterOnKey":
			if (openFilterOptions()) {
				if (condition.equals(">") || condition.equals("<"))
					if(!local_enabled)
						for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes()) {
							try {
								for (RecordFx record : node.getFromRDD().getRecords()) 
									Double.parseDouble(record.getKey().toString());
							} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
						}
				
				currentSystem.filter(condition, value, true);
				currentSystem.setRate(speed_value);
				currentSystem.getCurrentTransition().play();
				currentSystem.setSystemName("FilterOnKey");
			}
			break;
		case "FilterOnValue":
			if (openFilterOptions()) {
				if (condition.equals(">") || condition.equals("<"))
					if(!local_enabled)
						for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes()) {
							try {
								for (RecordFx record : node.getFromRDD().getRecords()) 
									Double.parseDouble(record.getValue().toString());
							} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
						}
				currentSystem.filter(condition, value, false);
				currentSystem.setRate(speed_value);
				currentSystem.getCurrentTransition().play();
				currentSystem.setSystemName("FilterOnValue");
			}
			break;
		case "Split":
			currentSystem.split();
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("Split");
			
			break;
		case "FlatMapToPair":
			currentSystem.flatMapToPair();
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("FlatMapToPair");
			
			break;
	}

		switch (reduce_function) {
		case "Count":
			currentSystem.count(false);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("Count");
			break;
		
		case "MinOnKey":
			if(!local_enabled)
				for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes()) {
					try {
						for (RecordFx record : node.getFromRDD().getRecords()) 
							Double.parseDouble(record.getKey().toString());
					} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
				}
			currentSystem.min(false, true);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("MinOnKey");
			break;
		case "MinOnValue":
			if(!local_enabled)
				for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes()) {
					try {
						for (RecordFx record : node.getFromRDD().getRecords()) 
							Double.parseDouble(record.getValue().toString());
					} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
				}
			currentSystem.min(false, false);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("MinOnValue");
			break;
		
		case "MaxOnKey":
			if(!local_enabled)
				for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes()) {
					try {
						for (RecordFx record : node.getFromRDD().getRecords()) 
							Double.parseDouble(record.getKey().toString());
					} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
				}
			currentSystem.max(false, true);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("MaxOnKey");
			break;
		case "MaxOnValue":
			if(!local_enabled)
				for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes()) {
					try {
						for (RecordFx record : node.getFromRDD().getRecords()) 
							Double.parseDouble(record.getValue().toString());
					} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
				}
			currentSystem.max(false, false);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("MaxOnValue");
			break;
		
		case "SumOnKey":
			if(!local_enabled)
				for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes()) {
					try {
						for (RecordFx record : node.getFromRDD().getRecords()) 
							Double.parseDouble(record.getKey().toString());
					} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
				}
			currentSystem.sum(false, true, false);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("SumOnKey");
			break;
		case "SumOnValue":
			if(!local_enabled)
				for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes()) {
					try {
						for (RecordFx record : node.getFromRDD().getRecords()) 
							Double.parseDouble(record.getValue().toString());
					} catch (NumberFormatException n) { DistributedSystemFx.warning("Values must be numbers!"); return; }
				}
			currentSystem.sum(false, false, false);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("SumOnValue");
			break;
			
		case "GroupByKey":
			currentSystem.reduceByKey(reduce_function, done_button);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("GroupByKey");
			break;
			
		case "ReduceByKey + Count":
			currentSystem.reduceByKey(reduce_function, done_button);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("RBK + Count");
			break;
		
		
		case "ReduceByKey + Min":
			currentSystem.reduceByKey(reduce_function, done_button);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("RBK + Min");
			break;
		
		case "ReduceByKey + Max":
			currentSystem.reduceByKey(reduce_function, done_button);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("RBK + Max");
			break;
			
		case "ReduceByKey + Sum":
			currentSystem.reduceByKey(reduce_function, done_button);
			currentSystem.setRate(speed_value);
			currentSystem.getCurrentTransition().play();
			currentSystem.setSystemName("RBK + Sum");
			break;
		}
		
	}


	/**
	 * Creates a snapshot of the system, increases the phase
	 * and the result RDD becomes the working RDD of the new phase.
	 */
	@FXML
	void done() {
		if (!local_enabled)
			if (currentSystem.toString().startsWith("RBK") || currentSystem.toString().startsWith("GroupByKey")) {
				for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes())
					node.removeTempRDDs();
				
				((DistributedSystemFx) currentSystem).relocate();
			}
		
		int numVoid = 0;
		
		if (currentSystem instanceof LocalSystemFx) ((LocalSystemFx) currentSystem).setTitle(currentSystem.toString());
		
		if(!local_enabled)
			for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes())
				if (node.getToRDD().size() == 0) numVoid++;

		// if all the nodes 
		if (numVoid == Integer.parseInt(nodes.getText())) return;

		if (!local_enabled) ((DistributedSystemFx) currentSystem).overwriteFromRDD();

		TreeItem<SystemFx> crumb_currentSystem = new TreeItem<>(currentSystem);
		selectedStage.getChildren().clear();
		selectedStage.getChildren().add(crumb_currentSystem);
		stages.setSelectedCrumb(crumb_currentSystem);
		selectedStage = crumb_currentSystem;
		
		if(!local_enabled)
			for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes())
				if (node.getFromRDD().size() == 0)
	        		node.setColor(NodeFx.RED);
	        	else
	        		node.setColor(NodeFx.GREEN);
		
		setCurrentSystem(currentSystem.copy());
		
		initTransition();

		map_list.setValue("-");
		reduce_list.setValue("-");
		
		done_button.setDisable(true);
		run_button.setDisable(false);
		
		if (local_enabled) {
			canvas.setPrefWidth(canvas.getWidth() + 260);
			scrollpane.setHvalue(1);
		}
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
	 * Opens a window with the filter option. It should be called 
	 * only when the filter option is selected.
	 */
	private boolean openLocalSystemDialog() throws IOException {
		// Create the custom dialog.
		Dialog<String[]> dialog = new Dialog<>();
		dialog.setTitle("System Mode");

		dialog.getDialogPane().getButtonTypes().addAll(ButtonType.APPLY, ButtonType.CANCEL);

		GridPane grid = new GridPane();
		grid.setHgap(10);
		grid.setVgap(10);
		grid.setPadding(new Insets(20, 150, 10, 10));
		
		CheckBox check_local = new CheckBox("Local System");
		check_local.selectedProperty().addListener( (arg, oldVal, newVal) -> local_enabled = newVal);
		grid.add(check_local, 0, 1);

		dialog.getDialogPane().setContent(grid);

		// Convert the result to a condition-value pair when the OK button is clicked.
		dialog.setResultConverter(
				dialogButton -> {
					if (dialogButton == ButtonType.APPLY)
						if (local_enabled)
							return new String[] { "true" };
						else
							return new String[] { "false" };
					
					Platform.exit();
					System.exit(0);
					return null;
				});

		Optional<String[]> result = dialog.showAndWait();

		result.ifPresent(
				conditionValue -> { 
					local_enabled = Boolean.parseBoolean(conditionValue[0]);
					
					nodes.setDisable(local_enabled);
					blocksize.setDisable(local_enabled);
				}
				);

		return result.isPresent();
	}

	/**
	 * Resets all the structures.
	 */
	public void reset() {		
		if(!local_enabled) setCurrentSystem(new DistributedSystemFx(Integer.parseInt(nodes.getText()), 
				Integer.parseInt(blocksize.getText()), rowsize));
		else
			setCurrentSystem(new LocalSystemFx());
		
		/*if (!local_enabled)
			for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes())
				node.setVisibleBg(!local_enabled);*/
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
		run_button.setDisable(true);

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
					run_button.setDisable(false);
				});

		stages.setCrumbFactory(crumb -> {
				
				BreadCrumbButton crumbButton = new BreadCrumbButton(crumb.getValue() != null ? crumb.getValue().toString() : "");
				
				if (crumb.getValue().toString().startsWith("RBK") || crumb.getValue().toString().startsWith("GroupByKey"))
					crumbButton.setTextFill(Color.RED);
				
				return crumbButton;
		});
		
		canvas.setPrefWidth(canvas.getWidth() + 800);
		
		try {
			openLocalSystemDialog();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void speed(double value) {
		speed_label.setText(String.format("%.1fx", value));
		
		speed_value = value;
		
		if (currentSystem.getCurrentTransition() != null) 
			currentSystem.setRate(speed_value);
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
		fileChooser.getExtensionFilters().add(new ExtensionFilter("CSV", "*.csv"));
		fileChooser.setTitle("Import Dataset File");
		input_file = fileChooser.showOpenDialog(new Stage());
	}

	private void setCurrentSystem(SystemFx s) {
		currentSystem = s;
		canvas.getChildren().set(0, currentSystem);
		zoom(zoom_value);
		
		initTransition();
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
		
		if (local_enabled) {
			blocksize.setText("");
			nodes.setText("1");
		}

		orchestrator = new Orchestrator(Integer.parseInt(nodes.getText()));

		currentSystem = local_enabled ? new LocalSystemFx() : new DistributedSystemFx(Integer.parseInt(nodes.getText()), 
				Integer.parseInt(blocksize.getText()), rowsize);
		
		/*if (!local_enabled)
			for (NodeFx node : ((DistributedSystemFx) currentSystem).getNodes())
				node.setVisibleBg(!local_enabled);*/

		canvas.getChildren().add(currentSystem);
		
		// zoom(0.6);
	}

	public void setRowsize(int rowsize) {
		this.rowsize = rowsize;
	}
}

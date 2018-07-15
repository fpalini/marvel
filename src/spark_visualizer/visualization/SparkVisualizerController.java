package spark_visualizer.visualization;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;

import javafx.animation.FillTransition;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.GridPane;
import javafx.scene.paint.Color;
import javafx.util.Duration;
import javafx.util.Pair;
import scala.Tuple2;
import spark_visualizer.orchestrator.Orchestrator;
import spark_visualizer.visualization.sparkfx.DistributedSystemFx;
import spark_visualizer.visualization.sparkfx.ExecutorFx;

public class SparkVisualizerController implements Initializable {
	
	@FXML
	private AnchorPane drawingPane;

	@FXML
	private ChoiceBox<String> keyType, valueType;

	@FXML
	private ComboBox<String> map_list, reduce_list;
	
	@FXML
	private Slider zoom_slider;
	
	@FXML
	private Label zoom_value;
	
	@FXML
	private TextField dataSize, blockSize, nExecutors;

	private ArrayList<DistributedSystemFx> systemPhases = new ArrayList<>();
	
	private int phase;
	
	private FillTransition ft = new FillTransition(Duration.millis(500));
	
	private Orchestrator orchestrator;
	
	
	/**
	 * Resets the system, generates a new random dataset
	 * and partitions it among the executors.
	 */
	@FXML
	private void generate() {
		reset();

		List<Tuple2<String,String>> dataset = createDataset();

		systemPhases.get(phase).parallelize(dataset);
	}

	/**
	 * Generates a random dataset.
	 * 
	 * @return list of {@literal <key, value>} pairs, representing the dataset.
	 */
    private List<Tuple2<String,String>> createDataset() {
        orchestrator.createRandomRDD(keyType.getValue(), valueType.getValue(), Integer.parseInt(dataSize.getText()));
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
		for (ExecutorFx executor : systemPhases.get(phase).getExecutors())
			if (executor.getToRDD().getNumBlocks() > 0) return;
		
		switch (map_function) {
			case "Filter":
				if (openFilterOptions()) systemPhases.get(phase).filter(condition, value);
			 	break;
		}
		
		switch (reduce_function) {
			case "Count":
				systemPhases.get(phase).count();
				break;
			case "Min":
				systemPhases.get(phase).min();
				break;
			case "Max":
				systemPhases.get(phase).max();
				break;
			case "Sum":
				systemPhases.get(phase).sum();
				break;
		}
	}

    
    /**
	 * Creates a snapshot of the system, increases the phase
	 * and the result RDD becomes the working RDD of the new phase.
	 */
	@FXML
    void done() {
        systemPhases.add(systemPhases.get(phase).copy()); // snapshot!
        nextPhase();
        systemPhases.get(phase).overwriteFromRDD();
    }
	
	
	/**
	 * Decrease the phase of the system.
	 */
	@FXML
	void prevPhase()
	{
		phase -= phase == 0 ? 0 : 1; 
		changeSystemPhase(phase);
	}
	
	
	/**
	 * Increase the phase of the system.
	 */
	@FXML
	void nextPhase()
	{
		phase += phase == systemPhases.size()-1 ? 0 : 1; 
		changeSystemPhase(phase);
	}
	
	
	/**
	 * Changes the actual view of the system.
	 * 
	 * @param index index of the system phase.
	 */
	public void changeSystemPhase(int index) {
		drawingPane.getChildren().clear();
		drawingPane.getChildren().add(systemPhases.get(index));
	}
	
	
	

	private String condition = ">", value = "0";

	/**
	 * Opens a window with the filter option. It should be called 
	 * only when the filter option is selected.
	 */
	public boolean openFilterOptions() throws IOException {
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
		condition.getItems().add("not null");

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
	
	/**
	 * Resets all the structures.
	 */
	@FXML
	public void reset() {
		drawingPane.getChildren().clear();
		systemPhases.clear();
		phase = 0;
		
		systemPhases.add(new DistributedSystemFx(Integer.parseInt(nExecutors.getText()), 
				Integer.parseInt(blockSize.getText())));
		
		drawingPane.getChildren().add(systemPhases.get(phase));
	}
	
	@Override
	public void initialize(URL location, ResourceBundle resources) {
		orchestrator = new Orchestrator(Integer.parseInt(nExecutors.getText()));
		
		map_list.getItems().add("-");
		map_list.getItems().add("Filter");
		
		reduce_list.getItems().add("-");
		reduce_list.getItems().add("Count");
		reduce_list.getItems().add("Min");
		reduce_list.getItems().add("Max");
		reduce_list.getItems().add("Sum");
		
		map_list.setValue("-");
		reduce_list.setValue("-");

		keyType.getItems().add("String");
        keyType.getItems().add("Integer");
        keyType.getItems().add("Double");
        keyType.getItems().add("-");

        valueType.getItems().add("String");
        valueType.getItems().add("Integer");
        valueType.getItems().add("Double");

        keyType.setValue("String");
        valueType.setValue("Integer");

        systemPhases.add(new DistributedSystemFx(Integer.parseInt(nExecutors.getText()), Integer.parseInt(blockSize.getText())));
		
		drawingPane.getChildren().add(systemPhases.get(phase));
		
		ft.setToValue(Color.YELLOW);
		ft.setCycleCount(2);
	    ft.setAutoReverse(true);
		
		zoom_slider.valueProperty().addListener(
				(observable, oldValue, newValue) -> {
					zoom_value.setText(String.format("%.1fx", newValue.doubleValue()/100));
					
					double scale_value = newValue.doubleValue()/100;

					for (int p = 0; p < systemPhases.size(); p++) {
	                    systemPhases.get(p).setScaleX(scale_value);
	                    systemPhases.get(p).setScaleY(scale_value);

	                    double newWidth = systemPhases.get(p).width() * scale_value;
	                    double newHeight = systemPhases.get(p).height() * scale_value;

	                    drawingPane.setPrefWidth(newWidth);
	                    drawingPane.setPrefHeight(newHeight);

	                    systemPhases.get(p).setTranslateX((newWidth - systemPhases.get(p).width())/2 + 100);
	                    systemPhases.get(p).setTranslateY((newHeight - systemPhases.get(p).height())/2 + 50);
	                }
				}
		);
	}
}

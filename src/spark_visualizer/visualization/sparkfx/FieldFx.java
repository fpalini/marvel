package spark_visualizer.visualization.sparkfx;

import javafx.scene.layout.StackPane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.util.Duration;
import javafx.animation.FillTransition;
import javafx.scene.control.Label;

/**
 * Class representing a field of an RDD record, using components of JavaFX.
 * 
 * @author Francesco Palini
 */

public class FieldFx extends StackPane {

	/**
     * Time in ms for the animations.
     */
    static public final int ANIMATION_MS = 500;
	
    public static final double WIDTH = 60, HEIGHT = 20;

    private Label text;
    private Rectangle cell;
    private FillTransition colorChange;
    

    public FieldFx(String value) {
        text = new Label(value);

        cell = new Rectangle(WIDTH, HEIGHT, Color.CADETBLUE);
        cell.setStroke(Color.BLACK);

        colorChange = new FillTransition(Duration.millis(ANIMATION_MS), cell);
        colorChange.setToValue(Color.YELLOW);

        getChildren().addAll(cell, text);

        setPrefWidth(WIDTH);
        setPrefHeight(HEIGHT);
    }

    
    public Label getLabel() { return text; }
    
    public FillTransition getColorChange() { return colorChange; }
    
    public Color getColor() { return (Color) cell.getFill(); }
    
    public void setColor(Color color) { cell.setFill(color); }
    
    @Override
    public String toString() { return text.getText(); }
}

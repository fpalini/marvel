package marvel.visualization.sparkfx;

import javafx.scene.layout.StackPane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.util.Duration;
import javafx.animation.FillTransition;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;

/**
 * Class representing a field of an RDD record, using components of JavaFX.
 * 
 * @author Francesco Palini
 */

public class FieldFx extends StackPane implements Comparable<FieldFx> {

	public static final double HEIGHT = 20;
	public static final double DEFAULT_WIDTH = 80;
	private static double width = DEFAULT_WIDTH;

    private Label text;
    private Rectangle cell;
    private FillTransition colorChange;

	/**
	 * Time in ms for the animations.
	 */
	static public int ANIMATION_MS = 350;
    

    public FieldFx(String value) {
        text = new Label(value);

        cell = new Rectangle(width, HEIGHT, Color.CYAN);
        cell.setStroke(Color.BLACK);
        Tooltip.install(cell, new Tooltip(value));
        Tooltip.install(text, new Tooltip(value));

        colorChange = new FillTransition(Duration.millis(ANIMATION_MS), cell);
        colorChange.setToValue(Color.YELLOW);

        getChildren().addAll(cell, text);

        setPrefWidth(width);
        setPrefHeight(HEIGHT);
    }

    public void setText(String s) { text.setText(s); Tooltip.install(text, new Tooltip(s)); }
    
    public Label getLabel() { return text; }
    
    public FillTransition getColorChange() { return colorChange; }
    
    public Color getColor() { return (Color) cell.getFill(); }
    
    public void setColor(Color color) { cell.setFill(color); }
    
    @Override
    public String toString() { return text.getText(); }
    
    @Override
    public boolean equals(Object o) {
    	return ((FieldFx) o).text.getText().equals(text.getText());
    }

	public Node getCell() {
		return cell;
	}
	
	@Override
	public int compareTo(FieldFx f) {
		return toString().compareTo(f.toString());
	}
	
	public static void set_width(double width) {
		FieldFx.width = width;
	}
	
	public static double get_width() { return width; }
}

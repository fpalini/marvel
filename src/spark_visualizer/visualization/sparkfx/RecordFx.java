package spark_visualizer.visualization.sparkfx;

import javafx.animation.FadeTransition;
import javafx.animation.ParallelTransition;
import javafx.animation.Transition;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.util.Duration;

/**
 * Class representing a record of an RDD record, using components of JavaFX.
 * 
 * @author Francesco Palini
 */

public class RecordFx extends HBox {

    public static final double HEIGHT = FieldFx.HEIGHT;

    private FieldFx key, value;
    private double width;
    private FadeTransition fadeIn;
    private FadeTransition fadeOut;
    

    public RecordFx(String k, String v) {
        fadeIn = new FadeTransition(Duration.millis(2 * FieldFx.ANIMATION_MS), this);
        fadeIn.setFromValue(0.0);
        fadeIn.setToValue(1.0);
        
        fadeOut = new FadeTransition(Duration.millis(2 * FieldFx.ANIMATION_MS), this);
        fadeOut.setFromValue(1.0);
        fadeOut.setToValue(0.0);

        value = new FieldFx(v);
        width = FieldFx.WIDTH;
        
        // the key can be null, it implies an RDD with only values
        if (k != null) {
            key = new FieldFx(k);
            getChildren().add(key);
            width *= 2;
        }
        
        getChildren().add(value);
    }

    
    public Transition getColorChange() {
        ParallelTransition parallelTransition = new ParallelTransition();

        // used for the "searching" animation
        parallelTransition.setCycleCount(2);
        parallelTransition.setAutoReverse(true);
        
        parallelTransition.getChildren().add(value.getColorChange());
        if (key != null) parallelTransition.getChildren().add(key.getColorChange());

        return parallelTransition;
    }

    
    public RecordFx copy() {
        String keyText = key == null ? null : key.toString();

        RecordFx record = new RecordFx(keyText, value.toString());
        record.setColor(getColor());

        return record;
    }
    
    
    public FadeTransition getFadeIn() { return fadeIn; }
    
    public FadeTransition getFadeOut() { return fadeOut; }
    
    public double width() { return width; }
    
    public double height() { return HEIGHT; }
    
    public FieldFx getKey() { return key; }
    
    public FieldFx getValue() { return value; }
    
    public void setKey(FieldFx f) { key = f; }
    
    public void setValue(FieldFx f) { value = f; }
    
    public void setColor(Color color) { value.setColor(color); }
    
    // assumption: key and value have the same color
    public Color getColor() { return value.getColor(); }
    
    @Override
    public String toString() {
    	return "(" + (key != null? key.toString() + ", " : "") + value.toString() + ")";
    }
    
    public void swap() {
    	value.toBack();
    	
    	FieldFx tmp = key;
    	key = value;
    	value = tmp;
    }
}

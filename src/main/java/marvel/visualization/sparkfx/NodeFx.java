package marvel.visualization.sparkfx;

import javafx.animation.Transition;
import javafx.scene.Group;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;

public class NodeFx extends Group {

	public final static Color GREEN = new Color(179/255.0, 255/255.0, 179/255.0, 1);
	public final static Color RED = new Color(255/255.0, 128/255.0, 128/255.0, 1);
    public static final double RDD_PADDING = 10;
    private static double WIDTH = RDD_PADDING + 2 * (RDD_PADDING + 2 * FieldFx.get_width());
    
    private double height, width;
    private Rectangle container;
    private RDDPartitionFx fromRDD, toRDD;
    private int id;
    private Label label;

    public NodeFx(double x, double y, int id) {

        height = width = WIDTH;
        this.id = id;

        container = new Rectangle(width, height);

        container.setFill(GREEN);
        container.setStroke(Color.BLACK);
        container.setStrokeWidth(1);
        container.setArcHeight(25);
        container.setArcWidth(25);
        label = new Label(id+"");
        label.setScaleX(WIDTH/15);
        label.setScaleY(WIDTH/15);
        label.setOpacity(0.75);
        
        fromRDD = new RDDPartitionFx();
		toRDD = new RDDPartitionFx();
		
		fromRDD.setLayoutY(RDD_PADDING);
		toRDD.setLayoutY(RDD_PADDING);
		
		fromRDD.setLayoutX(RDD_PADDING);
		toRDD.setLayoutX(2 * (RDD_PADDING + FieldFx.get_width()));

        getChildren().addAll(new StackPane(container, label), fromRDD, toRDD);

        setLayoutX(x);
        setLayoutY(y);
    }


    public NodeFx copy() {
        NodeFx node = new NodeFx(getLayoutX(), getLayoutY(), id);
        // node.setVisibleBg(container.isVisible());
        
        node.fromRDD.setBlocksize(fromRDD.getBlocksize());
        node.toRDD.setBlocksize(toRDD.getBlocksize());
        node.setColor((Color) container.getFill());

        for (RecordFx record : fromRDD.getRecords())
        	node.addRecordFromRDD(record.copy());
        
        for (RecordFx record : toRDD.getRecords())
        	node.addRecordToRDD(record.copy());

        return node;
    }
    
    public Transition addRecordFromRDD(RecordFx record) {
    	fromRDD.addRecord(record);
    	updateNodeHeight();
    	
    	return record.getFadeIn();
    }
    
    public Transition addRecordToRDD(RecordFx record) {
    	toRDD.addRecord(record);
    	updateNodeHeight();
    	
    	return record.getFadeIn();
    }
    
    public Transition removeRecordToRDD(RecordFx record) {
    	toRDD.removeRecord(record);
    	
    	updateNodeHeight();
    	
    	return record.getFadeOut();
    }
    
    private void updateNodeHeight() {
    	double maxHeight = 0;
    	RDDPartitionFx rdd;
    	
    	for (int c = 1; c < getChildren().size(); c++) {
    		rdd = (RDDPartitionFx) getChildren().get(c);
    		if (maxHeight < rdd.height())
    			maxHeight = rdd.height();
    	}
    	
    	height = maxHeight + 2*RDD_PADDING < WIDTH ? WIDTH : maxHeight + 2*RDD_PADDING;
    	container.setHeight(height);
    }

    // Used to manage the shuffle. It removes the temporary RDDs
	public RDDPartitionFx addTempRDD() {
		RDDPartitionFx temp = new RDDPartitionFx();
		temp.setBlocksize(fromRDD.getBlocksize());
		
		temp.setLayoutY(RDD_PADDING);
		
		temp.setLayoutX(toRDD.getLayoutX());
		toRDD.setLayoutX(toRDD.getLayoutX() + RDD_PADDING + 2 * FieldFx.get_width());
		
		getChildren().add(temp);
		
		container.setWidth(container.getWidth() + RDD_PADDING + 2 * FieldFx.get_width());
		width = container.getWidth();
		
		return temp;
	}


	public double width() {
		return width;
	}


	public void setToRDD(RDDPartitionFx temp) {
		toRDD = temp;
	}


	public void setFromRDD(RDDPartitionFx temp) {
		fromRDD = temp;
	}

	// Used to manage the shuffle. It removes all the temporary RDDs
	public void removeTempRDDs() {		
		toRDD.setLayoutX(2 * (RDD_PADDING + FieldFx.get_width()));
		
		getChildren().remove(3, getChildren().size());
		
		container.setWidth(WIDTH);
		width = WIDTH;
		
		updateNodeHeight();
	}

	public void setColor(Color c) {
		container.setFill(c);
	}

	// Used to manage the two thresholds of records' size
	public void recompute_width() {
		WIDTH = RDD_PADDING + 2 * (RDD_PADDING + 2 * FieldFx.get_width());
		width = WIDTH;
	}
	
	public void setVisibleBg(boolean condition) {
		container.setVisible(condition);
		label.setVisible(condition);
	}
	
	public double height() { return height; }
    public RDDPartitionFx getFromRDD() { return fromRDD; }
    public RDDPartitionFx getToRDD() { return toRDD; }
}

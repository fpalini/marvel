package spark_visualizer.visualization.sparkfx;

import javafx.animation.Transition;
import javafx.scene.Group;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;

public class NodeFx extends Group {

    public static final double RDD_PADDING = 10;
    public static final double WIDTH = RDD_PADDING + 2 * (RDD_PADDING + 2 * FieldFx.WIDTH);
    
    private double height, width;
    private Rectangle container;
    private RDDPartitionFx fromRDD, toRDD;

    public NodeFx(double x, double y) {

        height = width = WIDTH;

        container = new Rectangle(width, height);

        container.setFill(new Color(179/255.0, 255/255.0, 179/255.0, 1));
        container.setStroke(Color.BLACK);
        container.setStrokeWidth(1);
        container.setArcHeight(25);
        container.setArcWidth(25);
        
        fromRDD = new RDDPartitionFx();
		toRDD = new RDDPartitionFx();
		
		fromRDD.setLayoutY(RDD_PADDING);
		toRDD.setLayoutY(RDD_PADDING);
		
		fromRDD.setLayoutX(RDD_PADDING);
		toRDD.setLayoutX(2 * (RDD_PADDING + FieldFx.WIDTH));

        getChildren().addAll(container, fromRDD, toRDD);

        setLayoutX(x);
        setLayoutY(y);
    }


    public NodeFx copy() {
        NodeFx node = new NodeFx(getLayoutX(), getLayoutY());
        
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
    	// record.setVisible(false);
    	fromRDD.addRecord(record);
    	
    	updateNodeHeight();
    	
    	// record.getFadeIn().setOnFinished((event) -> record.setVisible(true));
    	
    	return record.getFadeIn();
    }
    
    public Transition addRecordToRDD(RecordFx record) {
    	// record.setVisible(false);
    	toRDD.addRecord(record);
    	
    	updateNodeHeight();
    	
    	// record.getFadeIn().setOnFinished((event) -> record.setVisible(true));
    	
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
    
    public double height() { return height; }
    public RDDPartitionFx getFromRDD() { return fromRDD; }
    public RDDPartitionFx getToRDD() { return toRDD; }

    int n = 0;

	public RDDPartitionFx addTempRDD() {
		RDDPartitionFx temp = new RDDPartitionFx();
		temp.setBlocksize(fromRDD.getBlocksize());
		
		temp.setLayoutY(RDD_PADDING);
		
		temp.setLayoutX(toRDD.getLayoutX());
		toRDD.setLayoutX(toRDD.getLayoutX() + RDD_PADDING + 2 * FieldFx.WIDTH);
		
		getChildren().add(temp);
		
		container.setWidth(container.getWidth() + RDD_PADDING + 2 * FieldFx.WIDTH);
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


	public void removeTempRDDs() {		
		toRDD.setLayoutX(2 * (RDD_PADDING + FieldFx.WIDTH));
		
		getChildren().remove(3, getChildren().size());
		
		container.setWidth(WIDTH);
		width = WIDTH;
		
		updateNodeHeight();
	}
	
	public void recomputeHeight() {
		
	}


	public void setColor(Color c) {
		container.setFill(c);
	}
}

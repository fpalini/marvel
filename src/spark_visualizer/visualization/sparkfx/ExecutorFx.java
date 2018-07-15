package spark_visualizer.visualization.sparkfx;

import javafx.scene.Group;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;

public class ExecutorFx extends Group {

    public static final double RDD_PADDING = 10;
    public static final double WIDTH = RDD_PADDING + 2 * (RDD_PADDING + 2 * FieldFx.WIDTH);
    
    private double height;
    private Rectangle container;
    private RDDPartitionFx fromRDD, toRDD;

    public ExecutorFx(double x, double y) {

        height = WIDTH;

        container = new Rectangle(WIDTH, height);

        container.setFill(Color.DEEPSKYBLUE);
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


    public ExecutorFx copy() {
        ExecutorFx executor = new ExecutorFx(getLayoutX(), getLayoutY());

        for (BlockFx block : fromRDD.getBlocks())
        	executor.fromRDD.addBlock(block.copy());
        
        for (BlockFx block : toRDD.getBlocks())
        	executor.toRDD.addBlock(block.copy());

        return executor;
    }
    
    
    public void addBlockFromRDD(BlockFx block) {
    	fromRDD.addBlock(block);
    	
    	if (fromRDD.height() > height - 2*RDD_PADDING) {
    		height = fromRDD.height() + 2*RDD_PADDING;
    		container.setHeight(height);
    	}
    }
    
    public void addBlockToRDD(BlockFx block) {
    	toRDD.addBlock(block);
    	
    	if (toRDD.height() > height - 2*RDD_PADDING) {
    		height = toRDD.height() + 2*RDD_PADDING;
    		container.setHeight(height);
    	}
    }
    
    public double height() { return height; }
    public RDDPartitionFx getFromRDD() { return fromRDD; }
    public RDDPartitionFx getToRDD() { return toRDD; }
}

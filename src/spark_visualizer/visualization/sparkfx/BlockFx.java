package spark_visualizer.visualization.sparkfx;

import java.util.ArrayList;
import java.util.List;

import javafx.scene.Group;
import javafx.scene.Node;

/**
 * Class representing a block of records of an RDD, using components of JavaFX.
 * 
 * @author Francesco Palini
 *
 */

public class BlockFx extends Group {

    private double height, width;
    

    /**
     * Adds a new record to the block.
     * @param record the new record to insert.
     */
    public void addRecord(RecordFx record) {
        int nRecords = getChildren().size();

        if (nRecords > 0) {
            // setting the coordinate y of the new record
            RecordFx prevRecord = (RecordFx) getChildren().get(nRecords - 1);
            record.setLayoutY(prevRecord.getLayoutY() + RecordFx.HEIGHT);
        } else
            // first record, it must be set the width of the block
            incrWidth(-width + record.width());

        getChildren().add(record);
        incrHeight(RecordFx.HEIGHT); // increasing the height of the block
    }

    
    /**
     * Removes the record of the block.
     * @param index index of the record of the block.
     */
    public void removeRecord(int index) {
        getChildren().remove(index);
        incrHeight(-RecordFx.HEIGHT); // decreasing the height of the block

        if (index < getChildren().size())
            // there is a another record after the deleted one
            for (int i = index; i < getChildren().size(); i++)
                // move up all the records after the deleted one
                getChildren().get(i).setLayoutY(-RecordFx.HEIGHT);
    }

    
    /**
     * It copies the block, generating a new {@code BlockFx}.
     * @return the Block 
     */
    public BlockFx copy() {
        BlockFx block = new BlockFx();

        for (RecordFx record : getRecords())
            block.addRecord(record.copy());

        return block;
    }
    
    
    /**
     * List of records of the block.
     * @return list of the records
     */
    public List<RecordFx> getRecords() {
        List<RecordFx> records = new ArrayList<>();

        for (Node n : getChildren())
            records.add((RecordFx) n);

        return records;
    }

    
    public double height() { return height; }
    
    public double width() { return width; }
    
    public void incrHeight(double dh) { height += dh; }
    
    public void incrWidth(double dw) { width += dw; }
    public int size() {return getChildren().size(); }
    
    @Override
    public String toString() {
    	String s = "[";
    	
    	if (size() == 0) return "[]";
    	
    	for (int i = 0; i < size()-1; i++)
    		s += getChildren().get(i).toString() + ", ";
    		
    	s += getChildren().get(size()-1) + "]";
    	
    	return s;
    }
}

package spark_visualizer.visualization.sparkfx;

import java.util.ArrayList;

import javafx.scene.Group;
import javafx.scene.Node;

public class RDDPartitionFx extends Group {
	
	public static double BLOCK_PADDING = 10;
	public static double WIDTH = 2 * FieldFx.WIDTH;
	private double height;

	
	public void addBlock(BlockFx block) {
		int nBlocks = getNumBlocks();

        if (nBlocks > 0) {
            // if it isn't the first block of the RDD, move down the block
        	BlockFx prevBlock = (BlockFx) getChildren().get(nBlocks - 1);
            block.setLayoutY(prevBlock.getLayoutY() + prevBlock.height() + BLOCK_PADDING);
            height += BLOCK_PADDING + block.height();
        }
        else height = block.height();

        getChildren().add(block);
    }
	
	public void removeBlock(int index) {
		BlockFx block = (BlockFx) getChildren().remove(index);
		height -= getNumBlocks() == 1 ? block.height() : BLOCK_PADDING + block.height();
		
		if (index < getChildren().size())
            // there is a another block after the deleted one
            for (int i = index; i < getChildren().size(); i++) {
                // move up all the blocks after the deleted one
                getChildren().get(i).setLayoutY(-(BLOCK_PADDING + block.height()));
                block = (BlockFx) getChildren().get(i);
            }
    }

    public ArrayList<BlockFx> getBlocks() {
        ArrayList<BlockFx> blocks = new ArrayList<>();

        for (Node b : getChildren())
            blocks.add((BlockFx) b);

        return blocks;
    }

    public ArrayList<RecordFx> getRecords() {
        ArrayList<RecordFx> records = new ArrayList<>();

        for (BlockFx block : getBlocks())
            records.addAll(block.getRecords());

        return records;
    }
    
    public void clear() {
    	height = 0;
    	getChildren().clear();
    }
    
    public int getNumBlocks() { return getChildren().size(); }
    public double height() { return height; }
	public int getNumRecords() { return getRecords().size(); }
	public boolean isEmpty() { return getChildren().isEmpty(); }
	public int size() {return getChildren().size(); }
	
	@Override
	public String toString() {
		String s = "{";
		
		if (size() == 0) return "{}";
    	
    	for (int i = 0; i < size()-1; i++)
    		s += getChildren().get(i).toString() + ", ";
    		
    	s += getChildren().get(size()-1) + "}";
    	
    	return s;
	}
}

package spark_visualizer.visualization.sparkfx;

import java.util.ArrayList;

import javafx.scene.Group;
import javafx.scene.Node;

public class RDDPartitionFx extends Group {
	
	public static double BLOCK_PADDING = 10;
	public static double DEFAULT_WIDTH = 2 * FieldFx.get_width();
	private double height;
	private int blocksize;
	
	
	public void addRecord(RecordFx record) {
		BlockFx lastBlock = size() == 0 ? null : (BlockFx) getChildren().get(size()-1);
		if (lastBlock != null && lastBlock.size() < blocksize) {
			lastBlock.addRecord(record);
			height += RecordFx.HEIGHT;
		}
		else {
			BlockFx newBlock = new BlockFx();
			newBlock.addRecord(record);
			addBlock(newBlock);
		}
	}
	
	private void addBlock(BlockFx block) {
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
	
	public void setBlocksize(int b) {
		blocksize = b;
	}

	public int getBlocksize() {
		return blocksize;
	}
	
	public RDDPartitionFx copy() {
		RDDPartitionFx rdd = new RDDPartitionFx();
		
		for (BlockFx block : getBlocks())
			rdd.addBlock(block);
		
		rdd.setBlocksize(blocksize);
		
		return rdd;
	}

	public void removeRecord(RecordFx record) {
		getChildren().remove(record);
		
		BlockFx block;
		
		for (Node n : getChildren()) {
			block = (BlockFx) n;
			
			if (block.contains(record))
				block.removeRecord(record);
		}
	}
}

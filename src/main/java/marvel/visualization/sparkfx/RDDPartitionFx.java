package marvel.visualization.sparkfx;

import java.util.ArrayList;

import javafx.scene.Group;
import javafx.scene.Node;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.text.Text;

public class RDDPartitionFx extends Group {
	
	public static double BLOCK_PADDING = 10;
	public static double DEFAULT_WIDTH = 2 * FieldFx.get_width();
	private double height;
	private int blocksize;
	private Text title;
	
	public RDDPartitionFx() {
		title = new Text("");
		title.setLayoutX(10);
		title.setLayoutY(-15);
		title.setFont(Font.font("Verdana", FontWeight.BOLD, 16));
		
		getChildren().add(title);
	}
	
	public void addRecord(RecordFx record) {
		BlockFx lastBlock = size() == 0 ? null : (BlockFx) getChildren().get(size());
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
        if (size() > 0) {
            // if it isn't the first block of the RDD, move down the block
        	BlockFx prevBlock = (BlockFx) getChildren().get(size());
            block.setLayoutY(prevBlock.getLayoutY() + prevBlock.height() + BLOCK_PADDING);
            height += BLOCK_PADDING + block.height();
        }
        else height = block.height();

        getChildren().add(block);
    }

    public ArrayList<BlockFx> getBlocks() {
        ArrayList<BlockFx> blocks = new ArrayList<>();

        for (Node b : getChildren())
        	if (b instanceof BlockFx)
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
    	getChildren().subList(1, size()+1).clear();
    }
    
    public double height() { return height; }
	public int getNumRecords() { return getRecords().size(); }
	public boolean isEmpty() { return size() == 0; }
	public int size() {return getChildren().size() - 1; }
	
	@Override
	public String toString() {
		String s = "{";
		
		if (size() == 0) return "{}";
    	
    	for (int i = 1; i < size(); i++)
    		s += getChildren().get(i).toString() + ", ";
    		
    	s += getChildren().get(size()) + "}";
    	
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
		
		rdd.setBlocksize(blocksize);
		
		for (RecordFx record : getRecords())
			rdd.addRecord(record.copy());
		
		rdd.setTitle(title.getText());
		
		return rdd;
	}

	public void removeRecord(RecordFx record) {
		getChildren().remove(record);
		
		BlockFx block;
		
		for (Node n : getChildren()) 
			if (n instanceof BlockFx) {
				block = (BlockFx) n;
				
				if (block.contains(record))
					block.removeRecord(record);
			}
	}

	public int indexOf(RecordFx r) {
		for (int i = 0; i < getRecords().size(); i++)
			if (getRecords().get(i) == r)
				return i;
		
		return -1;
	}

	public void setTitle(String struct_title) {
		title.setText(struct_title);
		if (title.getText().contains("RBK"))
			title.setFill(Color.RED);
	}
}

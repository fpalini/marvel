package marvel;

import org.apache.commons.cli.*;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.stage.Stage;
import marvel.visualization.MarvelController;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;


public class Main extends Application {
	
	private static String datasize;
	private static String blocksize;
	private static String nodes;
	private static String keytype;
	private static String valuetype;
	private static String input;
	private static String split;
	private static String keycol;
	private static String valuecol;
	private static int rowsize;
	

	@Override
	public void start(Stage primaryStage) {
		try {
			FXMLLoader loader = new FXMLLoader(getClass().getResource("/marvel.fxml"));
			Parent root = loader.load();
			
			MarvelController controller = loader.getController();
			if (input != null) controller.setFile(input);
			controller.setDatasize(datasize);
			controller.setBlocksize(blocksize);
			controller.setNodes(nodes);
			controller.setKeytype(keytype);
			controller.setValuetype(valuetype);
			controller.setSplit(split);
			controller.setKeycol(keycol);
			controller.setValuecol(valuecol);
			controller.setRowsize(rowsize);
			controller.initSystem();
			
			Scene scene = new Scene(root);
			scene.getStylesheets().add(getClass().getResource("/application.css").toExternalForm());
			
			primaryStage.setScene(scene);
			primaryStage.setTitle("MARVEL");
			primaryStage.getIcons().add(new Image(this.getClass().getResourceAsStream("/icon.png")));
			primaryStage.show();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Options options = new Options();

        Option[] option_array = new Option[] {
        		new Option("d", "datasize", true, "size of the dataset. "
        				+ "With value -1 the program reads all the records, when input is specified."),
        		new Option("b", "blocksize", true, "size of each block inside the system"),
        		new Option("n", "nodes", true, "number of nodes inside the system"),
        		new Option("k", "keytype", true, "type of the key of the random dataset"),
        		new Option("v", "valuetype", true, "type of the value of the random dataset"),
        		new Option("i", "input", true, "dataset file path"),
        		new Option("s", "split", true, "dataset file path"),
        		new Option("K", "keycol", true, "column number of the key inside the dataset file"),
        		new Option("V", "valuecol", true, "column number of the value inside the dataset file"),
        		new Option("r", "rowsize", true, "maximum number of nodes per row")
        };
        
        for (Option option : option_array) options.addOption(option);        

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        if (cmd.hasOption('h')) formatter.printHelp("utility-name", options);
		
        datasize = cmd.getOptionValue("d", "10");
        blocksize = cmd.getOptionValue("b", "3");
        nodes = cmd.getOptionValue("n", "2");
        keytype = cmd.getOptionValue("k", "String");
        valuetype = cmd.getOptionValue("v", "Integer");
        input = cmd.getOptionValue("i");
        split = cmd.getOptionValue("s", ",");
        keycol = cmd.getOptionValue("K", "1");
		valuecol = cmd.getOptionValue("V", "2");
		rowsize = Integer.parseInt(cmd.getOptionValue("r", "6"));
		
		launch(args);
	}
}

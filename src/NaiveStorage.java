import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

class NaiveStorage {

    private List<double[]> root;
    private int depth;

    NaiveStorage(String fileName, int depth) {
        this(fileName);
        this.depth = depth;
    }

    NaiveStorage(String fileName) {

        root = new LinkedList<>();
        readData(fileName);
    }

    /**
     * Return a list of stored tuples
     */
    List<double[]> getRoot(){return root;}
    
    /**
     * Read the .csv file into the database
     **/
    private void readData(String fileName){
        File text = new File(fileName);
        try {
            Scanner scanner = new Scanner(text);
            while(scanner.hasNextLine()){
                String[] line = scanner.nextLine().split(",");
                insert(line);
                this.depth = Math.max(depth, line.length);
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Insert a tuple to the root
     */
    private void insert(String[] list) {
        double[] doubleValues = Arrays.stream(list)
                .mapToDouble(Double::parseDouble)
                .toArray();
        root.add(doubleValues);
    }

}
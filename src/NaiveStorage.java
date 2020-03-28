import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

class NaiveStorage {


    private List<double[]> root;
    private int depth;

    NaiveStorage(String fileName) {
        root = new LinkedList<>();
        depth = 0;
        readData(fileName);
    }

    NaiveStorage(String fileName, int depth) {
        root = new LinkedList<>();
        depth = 0;
        readData(fileName);
    }

    List<double[]> getRoot(){return root;}

    /**
     * Build the Trie
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
     * Insert a tuple into the trie
     **/
    private void insert(String[] list) {
        double[] doubleValues = Arrays.stream(list)
                .mapToDouble(Double::parseDouble)
                .toArray();
        root.add(doubleValues);
    }

    /**
     * DFS: Trace down every tuples in the database storage
     * Print all tuples in the database
     **/
    void displayAll(){
        for(double[] d : root)
            System.out.println(Arrays.toString(d));
    }



}
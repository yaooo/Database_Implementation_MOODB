import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

class Trie {
    class TrieNode {
        private Map<Double, TrieNode> children = new HashMap<>();
        Map<Double, TrieNode> getChildren() {
            return children;
        }
    }

    private TrieNode root;
    private int depth;
    private long size; // directly return sum(1)

    Trie(String fileName) {
        root = new TrieNode();
        depth = 0;
        size = 0;
        readData(fileName);
    }

    Trie(String fileName, int depth) {
        root = new TrieNode();
        this.depth = depth;
        size = 0;
        readData(fileName);
    }

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
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * Insert a tuple into the trie
     **/
    void insert(String[] list) {
        TrieNode current = root;

        if(depth == 0) depth = Math.max(depth, list.length);

        for (String num: list) {
            double i = Double.parseDouble(num);
            if(!current.getChildren().containsKey(i)){
                current.getChildren().put(i, new TrieNode());
            }
            current = current.getChildren().get(i);
        }
    }

    /**
     * DFS: Trace down every tuples in the database storage
     * Print all tuples in the database
    **/
    void displayAll(){
        display(this.root, 0, new double[depth] );
    }

    private void display(TrieNode root, int level, double[] str){
        if(root == null || root.getChildren() == null || root.getChildren().isEmpty()){
            System.out.println(Arrays.toString(str));
            return;
        }

        for(double key: root.getChildren().keySet()){
            TrieNode next = root.getChildren().get(key);
            if(next != null){
                str[level] = key;
                display(next, level + 1, str);
            }
        }
    }

    TrieNode getRoot(){return root;}

    /**
     * Get Max depth of the trie (# of variables in this schema)
     **/
    int getDepth(){return depth;}


    /**
     * return if the Trie is empty
     * **/
    boolean isEmpty() {
        return root == null;
    }


    /**
     * Return the total number of tuples in the trie
     * **/
    long getSize(){return this.size;}

}
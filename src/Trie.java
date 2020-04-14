import java.io.File;
import java.io.FileNotFoundException;
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

    Trie(String fileName){
        clear();
        root = new TrieNode();
        readData(fileName);
    }

    Trie(String fileName, int depth) {
        this(fileName);
        this.depth = depth;
    }

    // clear the Trie
    private void clear(){
        root = null;
        depth = 0;
    }

    /**
     * Build the Trie given a filename
     **/
    public long readData(String fileName){
        long c = System.currentTimeMillis();
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
        return System.currentTimeMillis() - c;
    }

    /**
     * Insert a tuple into the trie, update the max depth of the trie
     **/
    private void insert(String[] list) {
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
        long c = System.currentTimeMillis();
        display(this.root, 0, new double[depth] );
        System.out.println(System.currentTimeMillis() - c);
    }

    // A helper function for the function "displayAll()"
    private void display(TrieNode root, int level, double[] str){
        if(root == null || root.getChildren() == null || root.getChildren().isEmpty()){
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

    /**
     * Return the root of the trie
     */
    TrieNode getRoot(){return root;}
}
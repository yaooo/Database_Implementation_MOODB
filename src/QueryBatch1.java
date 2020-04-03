import java.util.ArrayList;

public class QueryBatch1 {
    private ArrayList<Query> queries;
    private Schema schema;
    private int depth;
    private int version;

    QueryBatch1(Schema schema){
        this.schema = schema;
        this.depth = this.schema.getAttributeOrder().size();
        queries = new ArrayList<>();
        this.version = 1;
    }

    /**
     * Create query objects into a list
     */
    void readQueries(ArrayList<String> queries){
        for(String s: queries){
            this.queries.add(new Query(s.toUpperCase()));
        }
    }

    /**
     * Evaluate batch query and output the time taken
     */
    void evaluate(){
        long c = System.currentTimeMillis();
        traverse(this.schema.getTrie().getRoot(), 0, new double[depth] );
        if(Main.printResult)
            for(Query q: this.queries) {
                q.printResult();
            }
        System.out.println("Evaluate (MoonDB--version "+ version+ "), run time: " + (System.currentTimeMillis() - c) + "ms." );
    }


    /**
     * Traversing through the trie, compute batch query only at the leaf node of the trie
     */
    private void traverse(Trie.TrieNode root, int level, double[] str){
        if(root == null || root.getChildren() == null || root.getChildren().isEmpty()){
            for(Query q: this.queries){
                operation(q, str);            // perform calculation
            }
            return;
        }
        for(double key: root.getChildren().keySet()){
            Trie.TrieNode next = root.getChildren().get(key);
            if(next != null){
                str[level] = key;
                traverse(next, level + 1, str);
            }
        }
    }


    /**
     * Compute the return values for each tuple, and update return values for the query
     */
    private void operation(Query query, double[] str){
        double increment = 0;
        double key = (query.isGroupBy()) ? str[schema.fieldIndex(query.getGroupBy_Field())] : 0;

        for(int index = 0; index < query.getFieldSize(); index++) {
            String op = query.getFields().get(index);
            boolean ifSet = false; // for example, cases like "select A from R group by A", where we have to set B only

            if (op.equals("SUM(1)")) {
                increment = 1;

            } else if (op.contains("SUM")) {
                String expr = op.substring(4, op.length() - 1);
                increment = parseSum(expr, str);

            } else {
                // when only one letter is selected, eg. select A, B, C from R
                increment = str[schema.fieldIndex(op)];
                ifSet = true;
            }
            query.updateField(key, index, increment, ifSet);
        }
    }

    /**
     * Calculate the sum of product for each increment: SUM(A*B), SUM(A*B*C), etc
     * */
    private double parseSum(String expr, double[] str){ // example: A*B
        double increment = 1;
        for(char c: expr.toCharArray()){
            if(c != '*'){
                increment *= str[schema.fieldIndex(c+"")];
            }
        }
        return increment;
    }
}

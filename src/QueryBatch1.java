import java.util.ArrayList;

public class QueryBatch1 {
    private ArrayList<Query> queries;
    private Schema schema;
    private int depth;
    private int version;

    QueryBatch1(Schema schema){
        this.schema = schema;
        this.depth = this.schema.getAttributeOrder().size();
        this.queries = new ArrayList<>();
        this.version = 1;
    }

    /**
     * Create query objects into a list
     */
    void readQueries(ArrayList<String> queries){
        this.queries.clear();
        for(String s: queries){
            this.queries.add(new Query(s.toUpperCase()));
        }
    }

    /**
     * Evaluate batch query 5 times in a batch and output the average time taken of the last four times
     */
    void evaluate(double times, boolean print){
        double avg = 0;
        for(int i = 0; i < times; i ++){

            long c = System.currentTimeMillis();
            traverse(this.schema.getTrie().getRoot(), 0, new double[depth]);

            if(i == 0 && times == 1) avg = ((System.currentTimeMillis() - c) / 1000.0);
            if(i != 0) avg += (System.currentTimeMillis() - c) / (times - 1.0) / 1000.0;
            if(i == 0 && print){
                for(Query q: this.queries) {
                    q.printResult();
                }
            }
        }
        System.out.println("Evaluate MoonDB--version "+ version+ " in a batch, run time: " + avg + "s." );
    }

    /**
     * Evaluate batch query 5 times independently and output the average time taken of the last four times
     */
    void evaluateIndependently(){
        long diff = 0;
        for(int i = 0; i < 5; i++) {

            for (Query q : this.queries) {
                traverseSingleQuery(this.schema.getTrie().getRoot(), 0, new double[depth], q);
            }
            if(i == 0) diff = System.currentTimeMillis();
        }
        double time = (System.currentTimeMillis() - diff) / 1000.0 / 4;
        System.out.println("Evaluate (MoonDB--version "+ version + ") independently, run time: " +  time + "ms." );
    }


    /**
     * Traversing through the trie, compute a batch of queries at the leaf node of the trie
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
     * Given a query, traversing through the trie, compute results at the leaf node of the trie
     */
    private void traverseSingleQuery(Trie.TrieNode root, int level, double[] str, Query q){
        if(root == null || root.getChildren() == null || root.getChildren().isEmpty()){
            operation(q, str);            // perform calculation
            return;
        }
        for(double key: root.getChildren().keySet()){
            Trie.TrieNode next = root.getChildren().get(key);
            if(next != null){
                str[level] = key;
                traverseSingleQuery(next, level + 1, str, q);
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

            if (op.equals("SUM(1)")) { // when select "SUM(1)"
                increment = 1;

            } else if (op.contains("SUM")) { // when select "SUM(expr)"
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
     * Calculate the increment for each SUM(EXPR): SUM(A*B), SUM(A), etc
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

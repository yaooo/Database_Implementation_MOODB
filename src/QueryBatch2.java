import java.util.ArrayList;
import java.util.Arrays;

public class QueryBatch2 {
    private ArrayList<Query> queries;
    private Schema schema;
    private int depth;
    private int version;

    QueryBatch2(Schema schema){
        this.schema = schema;
        this.depth = this.schema.getAttributeOrder().size();
        queries = new ArrayList<>();
        this.version = 2;
    }

    // read all queries
    void readQueries(ArrayList<String> queries){
        for(String s: queries){
            this.queries.add(new Query(s.toUpperCase()));
        }
    }

    void evaluate(){
        long c = System.currentTimeMillis();
        traverse(this.schema.getTrie().getRoot(), 0, new double[depth] );
        if(Main.printResult)
            for(Query q: this.queries) {
                q.printResult();
            }
        System.out.println("Evaluate (MoonDB--version "+ version + "), run time: " + (System.currentTimeMillis() - c) + "ms." );
    }

    /**
     * Traversing the whole trie:
     * When it reaches the most bottom of the trie, start calculating the partial aggregates for each query
     * Otherwise, perform calculations over the corresponding attributes accordingly at each level, reset the corresponding partial aggregates afterward
     * **/
    private void traverse(Trie.TrieNode root, int level, double[] str){
        if(root == null || root.getChildren() == null || root.getChildren().isEmpty() || level == this.depth){
            for(Query q: this.queries){
                calculatePartial(q, str);// calculate par_aggs_gb
                if(q.getGroupBy_Field().equals(schema.getAttributeOrder().get(depth-1))){
                    inner(q, str);
                }
            }
            return;
        }
        for(double key: root.getChildren().keySet()){
            Trie.TrieNode next = root.getChildren().get(key);
            if(next != null){
                str[level] = key;
                traverse(next, level + 1, str);

                for(Query q: this.queries){
                    if(!q.getGroupBy_Field().equals(schema.getAttributeOrder().get(depth-1))) {
                        calculate(q, str, level);
                    }
                }
                // calculate aggs_gb
            }
        }
    }


    /**
     * Calculate the changes at the level of the trie corresponding to certain attribute
     * Update the return values of this query at this level of the trie (eg. update aggs_gb_b when level == 1)
     * Only update aggs at the very top level of the trie
     * **/
    private void calculate(Query query, double[] str, int level){
        int indexOfKey = schema.fieldIndex(query.getGroupBy_Field());

        double key = (query.isGroupBy()) ? str[indexOfKey] : 0;

        if(query.getGroupBy_Field().equals(schema.getLastAttribute())) return; // groupby E
        if(query.getType() == Query.GENERALQUERY && level == 0){ // groupby A or aggs
            double increment = 0;
            for(int index = 0; index < query.getFieldSize(); index++) {
                String op = query.getFields().get(index);
                boolean ifset = false;

                if(!op.contains("SUM")){ // select A from ...
                    increment = query.par_aggs[0];
                    ifset = true;
                }
                else if (op.equals("SUM(1)")) { // sum(1)
                    increment = query.par_aggs[index];
                } else if (op.contains("SUM") && op.contains(schema.getAttributeOrder().get(0))) { // select sum(A*B)
                    increment = str[0] * query.par_aggs[index];
                } else if (op.contains("SUM")) { // SUM(B*C) , SUM(B)
                    increment = query.par_aggs[index];
                }
                query.updateField(-1, index, increment, ifset); // update the query return fields
            }
            // reset par_aggs
            query.resetPartial();
        }
        else if(level == indexOfKey) { // any other group by
            double increment = 0;
            for(int index = 0; index < query.getFieldSize(); index ++){
                boolean ifset = false;
                if(index == 0) ifset = true;
                increment = query.par_aggs[index];
                query.updateField(key, index, increment, ifset);
            }
            query.resetPartial();
        }
    }


    /**
     * Calculate the partial aggregates for each query
     * **/
    private void calculatePartial(Query query, double[] str){
        double key = (query.isGroupBy()) ? str[schema.fieldIndex(query.getGroupBy_Field())] : 0;

        // not calculate the most inner level
        if(query.getGroupBy_Field().equals(schema.getAttributeOrder().get(depth - 1))) return;
        int type = query.getType(); // GROUPBYQUERY = 1; GENERALQUERY = 0;

        if(type == Query.GROUPBYQUERY){
            for(int index = 0; index < query.getFieldSize(); index++) {
                String op = query.getFields().get(index);
                // parse the tokenized query, one word by another
                if(!op.contains("SUM")){ // select A from
                    query.par_aggs[index] = key;
                }
                else if (op.equals("SUM(1)")) { // select sum(1)
                    query.par_aggs[index] += 1;
                } else if (op.contains("SUM")) { // select sum(expr), expr can be a*b*c*d....
                    String expr = op.substring(4, op.length() - 1);
                    query.par_aggs[index] += parseSum(expr, str, true);
                }
            }
        }
        else if(type == Query.GENERALQUERY){
            for(int index = 0; index < query.getFieldSize(); index++) {
                String op = query.getFields().get(index);

                if (op.equals("SUM(1)")) {
                    query.par_aggs[index] += 1;
                } else if (op.contains("SUM")) {
                    String expr = op.substring(4, op.length() - 1);
                    query.par_aggs[index] += parseSum(expr, str, false);
                }
            }
        }
    }


    /**
     * There is no need to calculate partial aggregates for the most bottom level of the trie,
     * thus directly calculate the aggregates of the attribute corresponding to the most bottom level of the trie.
     * **/
    private void inner(Query query, double[] str){
        double increment = 0;
        double key = (query.isGroupBy()) ? str[schema.fieldIndex(query.getGroupBy_Field())] : 0;

        for(int index = 0; index < query.getFieldSize(); index++) {
            String op = query.getFields().get(index);
            boolean ifSet = false; // for example, cases like "select A, B from R group by A", where we have to set B only

            if (op.equals("SUM(1)")) {
                increment = 1;

            } else if (op.contains("SUM")) {

                String expr = op.substring(4, op.length() - 1);
                increment = parseSum(expr, str, true);

            } else {
                // assume only one letter is selected, for example： select A, B, C from R
                increment = str[schema.fieldIndex(op)];
                ifSet = true;

            }
//            System.out.println("KEY:"+key + query.getGroupBy_Field());
            query.updateField(key, index, increment, ifSet);
        }
    }

    /**
     * Help to calculate
     * Calculate the sum of product for each increment: SUM(A*B), SUM(A*B*C), etc
     * */
    private double parseSum(String expr, double[] str, boolean ifGroupby){
        double increment = 1;
        if(!ifGroupby){
            for(char c: expr.toCharArray()){
                if(schema.getAttributeOrder().get(0).equals(""+c)) continue;
                if(c != '*')    increment *= str[schema.fieldIndex(c+"")];
            }
        }else{
            for(char c: expr.toCharArray()){
                if(c != '*' ){
                    increment *= str[schema.fieldIndex(c+"")];
                }
            }
        }
        return increment;
    }
}

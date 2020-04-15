import java.util.ArrayList;

public class QueryBatch3{
    private ArrayList<Query> queries; // list of queries
    private Schema schema; // relation schema
    private int depth; // number of attributes in the schema
    private int version;
    private Query generalQuery; // non-group-by query
    private Query innermostQuery;// group-by query with group-by field = last element of the attribute order
    private Query outermostQuery; // group-by query with group-by field = first element of the attribute order

    QueryBatch3(Schema schema){
        this.schema = schema;
        this.depth = this.schema.getAttributeOrder().size();
        reset();
    }

    /**
     * Reset the parameters of this object
     */
    private void reset(){
        this.queries = new ArrayList<>();
        this.version = 3;
        this.outermostQuery = null;
        this.generalQuery = null;
        this.innermostQuery = null;
    }

    /**
     * Create query objects into a list
     * For each query "query_*" in the list, decide if its aggregate can be decomposed,
     * if so, store the index of the to_be_reused value to query_*.mark[index]
     * @param queries list of queries in string
     */
    void readQueries(ArrayList<String> queries){
        reset();
        for(String s: queries){ // pre-processing the queries and add them to a list
            s = s.toUpperCase();
            Query q = new Query(s);

            if(s.contains("GROUP BY "+ this.schema.getAttrByIndex(0))){
                outermostQuery = q;
            }
            if(s.contains("GROUP BY "+ this.schema.getLastAttribute())){
                innermostQuery = q;
            }
            if(q.getType() == Query.GENERALQUERY){
                this.queries.add(0, q);
                this.generalQuery = q;
            }else
                this.queries.add(q);
        }

        // handle the general query
        if(generalQuery != null && outermostQuery != null){
            // if both non-group-by query and the outermostQuery are not null,
            // we can then take advantage of the partial aggregates for the non-group-by query
            for(int index = 0; index < generalQuery.getFieldSize(); index++) {
                String op = generalQuery.getFields().get(index);

                if (op.equals("SUM(1)")) {
                    int i = outermostQuery.getFields().indexOf("SUM(1)");
                    if(i != -1){
                        generalQuery.mark[index] = i;
                    }

                } else if (op.contains("SUM")) {
                    String expr = op.substring(4, op.length() - 1);
                    int i = ifOutermosterQueryContains(expr);
                    if(i != -1) { // if the outermost query has the value that we can reuse
                        generalQuery.mark[index] = i;
                    }
                }
            }
        }

        // handle any other group-by queries
        for(Query q: this.queries){
            if(q != innermostQuery && q != outermostQuery && q != generalQuery){
                int indexOfSum_1 = q.indexOfSum_1();
                String groupByField = q.getGroupBy_Field();

                // if sum(1) does not exit, that means we cannot take advantage of partials
                if(indexOfSum_1 == -1) continue;

                // otherwise, try to decompose the aggregate
                for(int index = 0; index < q.getFieldSize(); index++) {
                    String op = q.getFields().get(index);

                    if (!op.equals("SUM(1)") && op.contains("SUM")) {
                        String expr = op.substring(4, op.length() - 1);
                        // if we can decompose this aggregate, then we mark it
                        if(schema.comparePriority(expr, groupByField)<0){
                            q.mark[index] = indexOfSum_1;
                        }
                    }
                }
            }
        }
    }


    /**
     * Evaluate batch query and output the time taken
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
     * Evaluate query independently and output the total time taken
     */
    double evaluateIndependently(){

        long c = System.currentTimeMillis();
        long diff = 0;
        for(int i = 0; i < 5; i++) {
            for (Query q : this.queries) {
                traverseSingleQuery(this.schema.getTrie().getRoot(), 0, new double[depth], q);
//                if(i == 0)
//                    q.printResult();
            }
            if(i == 0) diff = System.currentTimeMillis();
        }
        double time = (System.currentTimeMillis() - diff) / 1000.0 / 4;
        return time;
    }


    /**
     * Traversing the whole trie:
     * When it reaches the most bottom of the trie, start calculating the partial aggregates for each query
     * Otherwise, perform calculations over the corresponding attributes accordingly at each level, reset the corresponding partial aggregates afterward
     * **/
    private void traverse(Trie.TrieNode root, int level, double[] str){
        if(root == null || root.getChildren() == null || root.getChildren().isEmpty() || level == this.depth){
            for(Query q: queries){
                calculatePartial(q, str);// calculate par_aggs_gb
                if(q.equals(innermostQuery)){
                    inner(q, str);// directly calculate the most inner group-by query, when reaching the leaf of the Trie
                }
            }
            return;
        }
        for(double key: root.getChildren().keySet()){
            Trie.TrieNode next = root.getChildren().get(key);
            if(next != null){
                str[level] = key;
                traverse(next, level + 1, str);

                for(Query q: queries){
                    calculate(q, str, level); // calculate aggs_gb
                }
            }
        }
    }


    /**
     * Traversing the whole trie:
     * When it reaches the most bottom of the trie, start calculating the partial aggregates for each query
     * Otherwise, perform calculations over the corresponding attributes accordingly at each level, reset the corresponding partial aggregates afterward
     * **/
    private void traverseSingleQuery(Trie.TrieNode root, int level, double[] str, Query q){
        if(root == null || root.getChildren() == null || root.getChildren().isEmpty() || level == this.depth){
            calculatePartial(q, str);// calculate par_aggs_gb
            if(q.equals(innermostQuery)){
                inner(q, str);
            }
            return;
        }
        for(double key: root.getChildren().keySet()){
            Trie.TrieNode next = root.getChildren().get(key);
            if(next != null){
                str[level] = key;
                traverseSingleQuery(next, level + 1, str, q);
                calculate(q, str, level); // calculate aggs_gb
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

        // the innermost loop is already handled elsewhere, skip calculating the inner most query
        if(query.getGroupBy_Field().equals(schema.getLastAttribute())) return;

        // handle the outermost query or the non-group-by query. eg. groupby_A or aggs
        if(query.getType() == Query.GENERALQUERY && level == 0){
            double increment = 0;

            for(int index = 0; index < query.getFieldSize(); index++) {
                String op = query.getFields().get(index);
                boolean ifset = false;

                int markedIndex = query.mark[index];
                if(markedIndex != -1) increment = outermostQuery.par_aggs[markedIndex];

                if(!op.contains("SUM")){ // select A
                    increment = query.par_aggs[0];
                    ifset = true;
                } else if(op.equals("SUM(1)")) { // select sum(1)
                    increment = (markedIndex == -1)? query.par_aggs[index]:increment;
                    //  increment = query.par_aggs[index];
                } else if (op.contains("SUM") && op.contains(schema.getAttrByIndex(0))) { // sum(A*B)

                    increment = str[0] * ((markedIndex != -1)? increment:query.par_aggs[index]);
                    //  increment *= str[0] --->> value_of_group_by_field * query.par_aggs[index];

                } else if (op.contains("SUM")) { // SUM(B*C) , SUM(B)
                    increment = (markedIndex == -1) ? query.par_aggs[index] : increment;
                    //  increment = query.par_aggs[index];

                }
                query.updateField(-1, index, increment, ifset); // update the query return fields
            }
            query.resetPartial();// reset par_aggs
        }
        else if(level == indexOfKey) {// any other group-by-queries
            double increment = 0;
            for(int index = 0; index < query.getFieldSize(); index ++){
                String op = query.getFields().get(index);
                boolean ifset = (index == 0);

                if (query.mark[index] != -1) { // locate the sharing partial aggregates
                    String expr = op.substring(4, op.length() - 1);
                    int i = schema.getAttributeOrder().indexOf(expr);
                    increment = query.par_aggs[query.mark[index]] * str[i];
                }else // or directly get it from its own partial aggregate
                    increment = query.par_aggs[index];
                query.updateField(key, index, increment, ifset);
            }
            query.resetPartial();// reset par_aggs
        }
    }

    /**
     * Calculate the partial aggregates for each query,
     * except group-by query "aggs_gb_{attr_*}", where {attr_*} is the attribute corresponding to the bottom level of the trie.
     * **/
    private void calculatePartial(Query query, double[] str){
        double key = (query.isGroupBy()) ? str[schema.fieldIndex(query.getGroupBy_Field())] : 0;

        // not calculate the most inner level
        if(query.equals(innermostQuery)) return;

        for(int index = 0; index < query.getFieldSize(); index++) {
            if(query.mark[index] != -1) continue;

            String op = query.getFields().get(index);
            // parse the tokenized query, one word by another
            if(!op.contains("SUM")){ // select A, B, C ....
                query.par_aggs[index] = key;
            } else if (op.equals("SUM(1)")) {
                query.par_aggs[index] += 1;
            } else if (op.contains("SUM")) {
                String expr = op.substring(4, op.length() - 1);
                query.par_aggs[index] += parseSum(expr, str, query.isGroupBy());
            }
        }
    }


    /**
     * Calculate the aggs_gb_{attribute_*}, where {attribute_*} is the attribute corresponding to the bottom level of the trie.
     **/
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
                // assume only one letter is selected, for example: select A, B, C from R
                increment = str[schema.fieldIndex(op)];
                ifSet = true;
            }
            query.updateField(key, index, increment, ifSet);
        }
    }

    /**
     * Calculate the expr
     * eg. expr can be SUM(A*B), SUM(A*B*C), etc
     * */
    private double parseSum(String expr, double[] str, boolean ifGroupby){
        double increment = 1;
        if(!ifGroupby){
            for(char c: expr.toCharArray()){
                if(schema.getAttrByIndex(0).equals(""+c)) continue;
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


    /**
     * If the outermost query contains this attribute
     * eg. if aggs_gb_A has "SUM(B)", and aggs tries to select "SUM(A*B)",
     * it returns the index where par_aggs_gb_A stores SUM(B),
     * otherwise it returns -1
     * */
    private int ifOutermosterQueryContains(String expr){
        boolean res = true;
        int res1 = -1;
        int count = 0;
        for(char c: expr.toCharArray()){
            if(c != '*'){
                boolean contains = (this.outermostQuery.getFields().indexOf("SUM("+c+")") != -1);
                res1 = this.outermostQuery.getFields().indexOf("SUM("+c+")");
                if(contains) count++;
                res = res &&((contains && count <2) || this.outermostQuery.getGroupBy_Field().equals(c+""));
            }
        }
        if(res){
            if(res1 == -1){ // sum(A)
                res1 = this.outermostQuery.getFields().indexOf("SUM(1)");
            }
        }
        return (res)? res1: -1;
    }
}

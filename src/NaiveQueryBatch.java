import java.util.ArrayList;

public class NaiveQueryBatch {
    private ArrayList<Query> queries;
    private NaiveSchema schema;

    NaiveQueryBatch(NaiveSchema schema){
        this.schema = schema;
        queries = new ArrayList<>();
    }

    /**
     * Load queriy strings into a list of query object
     */
    void readQueries(ArrayList<String> queries){
        this.queries.clear();
        for(String s: queries){
            Query q = new Query(s.toUpperCase());
            this.queries.add(q);
        }
    }

    /**
     * Evaluate batch query and output the time taken.
     */
    void evaluateBatch(double times, boolean print){
        NaiveStorage storage = schema.getNaiveStorage();
        double avg = 0;

        for(int i = 0; i < times; i ++) {
            for(Query q: this.queries) q.resetQuery();

            long c = System.currentTimeMillis();
            for (double[] d: storage.getRoot())
                for (Query q : this.queries) {
                    operation(q, d);
                }
            if(i == 0 && times == 1) avg = ((System.currentTimeMillis() - c) / 1000.0);

            if(i != 0) avg += (System.currentTimeMillis() - c) / (times-1) / 1000.0;
        }
        if(print) {
            System.out.println("Result from executing queries in a batch:");
            for (Query q : this.queries)  q.printResult();
        }
        System.out.println("Evaluate the queries in a batch, average run time: " + avg + "s." );
    }

    /**
     * Evaluate all queries, but one query at a time. Output the time taken.
     */
    void evaluateIndependently(double times){
        double avg = 0;
        for(int i = 0; i < times; i ++){
            for (Query q : this.queries) {
                q.resetQuery();
            }
            long c = System.currentTimeMillis();
            NaiveStorage storage = schema.getNaiveStorage();
            for (Query q : this.queries) {
                q.resetQuery();
                for (double[] d : storage.getRoot())
                    operation(q, d);
            }
            if(i != 0) avg += (System.currentTimeMillis() - c) / (times-1) / 1000.0;
        }
        System.out.println("Evaluate the queries independently, average run time: " + avg + "s." );
    }

    /**
     * Compute the return values for each tuple, and update return values for the query
     */
    private void operation(Query query, double[] str){
        double increment = 0;
        double key = (query.isGroupBy()) ? str[schema.fieldIndex(query.getGroupBy_Field())] : 0;

        for(int index = 0; index < query.getFieldSize(); index++) {
            String op = query.getFields().get(index);
            boolean ifSet = false; // for example, cases like "select A, B from R group by A", where we have to set B only

            if (op.equals("SUM(1)")) {
                increment = 1;

            } else if (op.contains("SUM")) {
                String expr = op.substring(4, op.length() - 1);
                increment = parseSum(expr, str);

            } else {
                // assume only one letter is selected, for example: select A, B, C from R
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

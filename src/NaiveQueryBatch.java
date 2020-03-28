import java.awt.*;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;

public class NaiveQueryBatch {
    private ArrayList<Query> queries;
    private NaiveSchema schema;
    private int depth;

    public NaiveQueryBatch(NaiveSchema schema){
        this.schema = schema;
        this.depth = this.schema.getAttributeOrder().size();
        queries = new ArrayList<>();
    }

    // read all queries
    void readQueries(ArrayList<String> queries){
        for(String s: queries){
            this.queries.add(new Query(s));
        }
    }

    ArrayList<Query> getQueries(){return queries;}

    void evaluateBatch(){
        long c = System.currentTimeMillis();
        NaiveStorage storage = schema.getNaiveStorage();
        for(double[] d : storage.getRoot()) {
            for (Query q : this.queries) {
                operation(q, d);
            }
        }
//        for (Query q : this.queries) {
//            q.printResult();
//        }
        System.out.println("Evaluate the query batch, run time: " + (System.currentTimeMillis() - c) + "ms." );

    }

    void evaluateIndepently(){
        long c = System.currentTimeMillis();
        NaiveStorage storage = schema.getNaiveStorage();
        for (Query q : this.queries) {
            for(double[] d : storage.getRoot()) {
                operation(q, d);
            }
        }
//        for (Query q : this.queries) {
//            q.printResult();
//        }
        System.out.println("Evaluate the query independently, run time: " + (System.currentTimeMillis() - c) + "ms." );
    }



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
                // assume only one letter is selected, for example： select A, B, C from R
                increment = str[schema.fieldIndex(op)];
//                System.out.println("Select A:"+ increment);
                ifSet = true;

            }
//            System.out.println("KEY:"+key + query.getGroupBy_Field());
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

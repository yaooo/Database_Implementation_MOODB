import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class QueryBatch2 {
    private ArrayList<Query> queries;
    private Schema schema;
    private int depth;

    public QueryBatch2(Schema schema){
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

    void evaluate(){
        long c = System.currentTimeMillis();
        traverse(this.schema.getTrie().getRoot(), 0, new double[depth] );
//        for(Query q: this.queries) {
//            q.printResult();
//        }
        System.out.println("Evaluate (MoonDB--version 2), run time: " + (System.currentTimeMillis() - c) + "ms." );
    }



    private void traverse(Trie.TrieNode root, int level, double[] str){

        if(root == null || root.getChildren() == null || root.getChildren().isEmpty()){
            for(Query q: this.queries){
                operation(q, str);// calculate par_aggs_gb
            }
            return;
        }

        for(double key: root.getChildren().keySet()){
            Trie.TrieNode next = root.getChildren().get(key);
            if(next != null){
                str[level] = key;
                traverse(next, level + 1, str);

                // calculate aggs_gb
            }
        }
    }


    // TODO: finish this part
    private void calculate(Query query, double[] str, int level){
        if(level == schema.fieldIndex(query.getGroupBy_Field())) {
            double increment = 0;
            double key = (query.isGroupBy()) ? str[schema.fieldIndex(query.getGroupBy_Field())] : 0;

            for (int index = 0; index < query.getFieldSize(); index++) {
                String op = query.getFields().get(index);
                boolean ifSet = false; // for example, cases like "select A, B from R group by A", where we have to set B only

                if (op.equals("SUM(1)")) {

                    increment = 1;

                } else if (op.contains("SUM")) {

                    String expr = op.substring(4, op.length() - 1);
//                    increment = parseSum(expr, str);

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
    }


    private void operation(Query query, double[] str){
        double increment = 0;
        double key = (query.isGroupBy()) ? str[schema.fieldIndex(query.getGroupBy_Field())] : 0;

        int type = query.getType(); // GROUPBYQUERY = 1; GENERALQUERY = 0;

        if(type == Query.GROUPBYQUERY){
            if(schema.fieldIndex(query.getGroupBy_Field()) == schema.getAttributeOrder().size() - 1){
                for(int index = 0; index < query.getFieldSize(); index++) {
                    String op = query.getFields().get(index);
                    boolean ifSet = false; // for example, cases like "select A, B from R group by A", where we have to set B only

                    if (op.equals("SUM(1)")) {
                        query.par_aggs[index] += 1;

                    } else if (op.contains("SUM")) {

                        String expr = op.substring(4, op.length() - 1);
                        increment = parseSum(expr, str, false);

                    } else {
                        increment = str[schema.fieldIndex(op)];
                        ifSet = true;

                    }
                    query.updateField(key, index, increment, ifSet);
                }
            }else{
                for(int index = 0; index < query.getFieldSize(); index++) {
                    String op = query.getFields().get(index);
                    // parse the tokenized query, one word by another
                    if (op.equals("SUM(1)")) {
                        query.par_aggs[index] += 1;
                    } else if (op.contains("SUM")) {
                        String expr = op.substring(4, op.length() - 1);
                        query.par_aggs[index] += parseSum(expr, str, true);
                    }
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
     * Skip the "A"s, for example (A*B) --> 1*B; (A) --> 1
     * Calculate the sum of product for each increment: SUM(A*B), SUM(A*B*C), etc
     * */
    private double parseSum(String expr, double[] str, boolean ifGroupby){
        double increment = 1;
        for(char c: expr.toCharArray()){
            if(c != '*' || (!ifGroupby && schema.getAttributeOrder().get(0).equals(""+c))){
                increment *= str[schema.fieldIndex(c+"")];
            }
        }
        return increment;
    }
}

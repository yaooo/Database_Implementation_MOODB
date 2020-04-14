import java.util.*;

public class Query {

    private String query;
    private int fieldSize; // number of fields to be returned
    private int type; //GROUPBYQUERY = 1; GENERALQUERY = 0;
    private HashMap<Double, double[]> aggs_groupby; // store the output fields of a group-by query
    private double[] aggs; // store the output fields of a non-group-by query
    private ArrayList<String> outputFieldNames; // store the names of the output fields of a query
    private String GroupBy_Field; // if a query is a group-by query, store its group-by field here

    int[] mark; // decide if a output field can be decomposed given an index
    // used in the second and third implementation of MooDB

    double[] par_aggs; // Partial aggregates for a query
    // used in the second and third implementation of MooDB

    final static int GROUPBYQUERY = 1;
    final static int GENERALQUERY = 0;

    // initialize a query
    Query(String s){
        s = s.toUpperCase();
        this.GroupBy_Field = "";
        this.query = s;
        this.type = QueryType(s);
        this.outputFieldNames = new ArrayList<>();
        this.parse();
        this.fieldSize = outputFieldNames.size();
        resetQuery();
    }

    /**
     * reset the query parameters
     */
    void resetQuery(){
        this.aggs_groupby = new HashMap<>();
        this.aggs = new double[fieldSize];
        this.par_aggs = new double[fieldSize];
        this.mark = new int[fieldSize];
        Arrays.fill(mark, -1);
        resetPartial ();
    }

    /**
     * Reset the partial aggregate array
     */
    void resetPartial (){
        this.par_aggs = new double[fieldSize];
    }

    /**
     * Update the return values of the query
     * @param key the group_by field for a group-by query, otherwise this value can be arbitrary
     * @param index the index of the updated position in the return_fields list
     * @param increment a value to be increased
     * @param ifSet whether to increase the value or set the value
     */
    void updateField(double key, int index, double increment, boolean ifSet){
        if(type == GROUPBYQUERY) {
            double[] fields = (aggs_groupby.containsKey(key)) ? aggs_groupby.get(key) : new double[fieldSize];
            if(!ifSet){
                fields[index] += increment;
                aggs_groupby.put(key, fields);
            }else{
                fields[index] = key;
                aggs_groupby.putIfAbsent(key, fields);
            }
        }
        if(type == GENERALQUERY){
            aggs[index] = ifSet ? increment: aggs[index] + increment;
        }
    }

    /**
     * Print the results of aggregates from this query
     */
    void printResult(){
        System.out.println("\nResult of " + this.query);
        if(type == GROUPBYQUERY){
            for(double key: aggs_groupby.keySet()){
                System.out.println(Arrays.toString(aggs_groupby.get(key)));
            }
        }
        if(type == GENERALQUERY){
            System.out.println(Arrays.toString(aggs));
        }
    }

    /**
     * Determine the type of the query
     * Type 0: contains "GROUP BY"
     * Type 1: not contains "GROUP BY"
     **/
    private int QueryType(String query){
        return query.contains("GROUP BY")? GROUPBYQUERY:GENERALQUERY;
    }

    /**
     * Parse the query, and store the information needed to identify the query
     */
    private void parse(){
        String theString = this.query.replace(';', ' ');
        int addField = 0;
        StringTokenizer st = new StringTokenizer(theString," ,");
        while (st.hasMoreTokens()){
            String theToken=st.nextToken();

            if(theToken.equals("SELECT")) continue;
            if(theToken.equals("FROM")){
                addField = 1;
                continue;
            }
            if(theToken.equals("BY")){
                addField = 2;
                continue;
            }
            if(addField == 0) this.outputFieldNames.add(theToken);
            if(addField == 2) this.GroupBy_Field = theToken;
        }
    }

    /**
     * @return if this query contains the "GROUP BY" keyword
     */
    boolean isGroupBy(){
        return this.type == GROUPBYQUERY;
    }

    /**
     * Return the index of "SUM(1)" from the list of return fields
     * Return -1 if "SUM(1)" not found
     */
    int indexOfSum_1(){
        for(int index = 0; index < fieldSize; index++) {
            String op = outputFieldNames.get(index);
            if (op.equals("SUM(1)")) {
                return  index;
            }
        }
        return -1;
    }

    // return a list of output fields
    ArrayList<String> getFields(){return outputFieldNames;}

    // return the number of output fields
    int getFieldSize(){return fieldSize;}

    // return the group-by field if this query is a group-by query, otherwise returns an empty string
    String getGroupBy_Field(){return GroupBy_Field;}

    // return the type of this query: GROUPBYQUERY = 1; GENERALQUERY = 0;
    int getType(){return type;}
}

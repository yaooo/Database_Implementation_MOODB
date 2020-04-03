import java.util.*;

public class Query {

    private String query;
    private int fieldSize; // number of fields to be returned
    private int type;
    private HashMap<Double, double[]> aggs_groupby;
    private double[] aggs;
    public int[] mark;


    public double[] par_aggs;

    private ArrayList<String> outputFieldNames;
    private String GroupBy_Field;

    final static int GROUPBYQUERY = 1;
    final static int GENERALQUERY = 0;


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

    void resetQuery(){
        this.aggs_groupby = new HashMap<>();
        this.aggs = new double[fieldSize];
        this.par_aggs = new double[fieldSize];
        this.mark = new int[fieldSize];
        Arrays.fill(mark, -1);
    }

    void resetPartial (){
        this.par_aggs = new double[fieldSize];
    }


    /**
     * For example: aggs_gb_A[A][0] += par_aggs_gb_A[0]
     * --> updateField(A, 0, par_aggs_gb_A[0])
     **/
    void updateField(double key, int index, double increment, boolean ifSet){

        if(type == GROUPBYQUERY) {
            double[] fields = (aggs_groupby.containsKey(key)) ? aggs_groupby.get(key) : new double[fieldSize];
            if(!ifSet){
                fields[index] += increment;
            }else{
                fields[index] = key;
            }
            aggs_groupby.put(key, fields);
        }
        if(type == GENERALQUERY){
            aggs[index] = ifSet ? increment: aggs[index] + increment;
        }
    }

    ArrayList<String> getFields(){return outputFieldNames;}

    int getFieldSize(){return fieldSize;}

    String getGroupBy_Field(){return GroupBy_Field;}

    int getType(){return type;}

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
//            System.out.println("Type:" + addField + "--"+ theToken);
        }
    }

    public void printQueryParser(){
        System.out.println(outputFieldNames.toString());
        if(this.type == GROUPBYQUERY)
            System.out.println(GroupBy_Field);
    }

    boolean isGroupBy(){
        return this.type == GROUPBYQUERY;
    }

    int indexOfSum_1(){
        for(int index = 0; index < fieldSize; index++) {
            String op = outputFieldNames.get(index);
            if (op.equals("SUM(1)")) {
                return  index;
            }
        }
        return -1;
    }

}

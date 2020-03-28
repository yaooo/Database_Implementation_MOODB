import java.util.*;

public class Main {

    // modify this variable for other csv files
    private final static String FILENAME = "dataset/sf1.csv";
    private static List<String> FILENAMES = new LinkedList<>();
    private final static String[] attributes = new String[]{"A", "B", "C", "D", "E"};


    public static void main(String[] args) {
        inputFileNames(15, 20);
        naiveDBBenchMark();
        moonDBBenchMark();
    }

    private static void naiveDBBenchMark(){
        for(String f: FILENAMES) {

            System.out.println("EXECUTING FILE: " + f);
            NaiveSchema schema = new NaiveSchema(f, Arrays.asList(attributes));
            NaiveQueryBatch qb = new NaiveQueryBatch(schema);

            // read queries
            ArrayList<String> queries = new ArrayList<>();
            queries.add("SELECT A, SUM(1), SUM(B), SUM(C), SUM(D), SUM(E) FROM R group by A;");

            queries.add("SELECT B, SUM(1), SUM(A), SUM(C), SUM(D), SUM(E) FROM R GROUP BY B;");
            queries.add("SELECT C, SUM(1), SUM(A), SUM(B), SUM(D), SUM(E) FROM R GROUP BY C;");
            queries.add("SELECT D, SUM(1), SUM(A), SUM(B), SUM(C), SUM(E) FROM R GROUP BY D;");
            queries.add("SELECT E, SUM(1), SUM(A), SUM(B), SUM(C), SUM(D) FROM R GROUP BY E;");
            queries.add("SELECT SUM(1), SUM(A), SUM(B), SUM(C), SUM(D), SUM(E)," +
                    "SUM(A*B), SUM(A*C), SUM(A*D), SUM(A*E)," +
                    "SUM(B*C), SUM(B*D), SUM(B*E)," +
                    "SUM(C*D), SUM(C*E)," +
                    "SUM(D*E) " +
                    "FROM R;");

            // read queries into query batch before processing further
            qb.readQueries(queries);
            qb.evaluateIndepently();

            qb.evaluateBatch();
        }
    }
    private static void moonDBBenchMark(){
        // create the trie and read data from the csv file
        for(String f: FILENAMES) {
            System.out.println("EXECUTING FILE: " + f);
            Schema schema = new Schema(f, Arrays.asList(attributes));
            Trie trie = schema.getTrie();

            QueryBatch qb = new QueryBatch(schema);

            // read queries
            ArrayList<String> queries = new ArrayList<>();
            queries.add("SELECT A, SUM(1), SUM(B), SUM(C), SUM(D), SUM(E) FROM R group by A;");

            queries.add("SELECT B, SUM(1), SUM(A), SUM(C), SUM(D), SUM(E) FROM R GROUP BY B;");
            queries.add("SELECT C, SUM(1), SUM(A), SUM(B), SUM(D), SUM(E) FROM R GROUP BY C;");
            queries.add("SELECT D, SUM(1), SUM(A), SUM(B), SUM(C), SUM(E) FROM R GROUP BY D;");
            queries.add("SELECT E, SUM(1), SUM(A), SUM(B), SUM(C), SUM(D) FROM R GROUP BY E;");
            queries.add("SELECT SUM(1), SUM(A), SUM(B), SUM(C), SUM(D), SUM(E)," +
                    "SUM(A*B), SUM(A*C), SUM(A*D), SUM(A*E)," +
                    "SUM(B*C), SUM(B*D), SUM(B*E)," +
                    "SUM(C*D), SUM(C*E)," +
                    "SUM(D*E) " +
                    "FROM R;");

            // read queries into query batch before processing further
            qb.readQueries(queries);

            // problem 1
            qb.evaluate1();


            //        // get all queries
            //        ArrayList<Query> queryList = qb.getQueries();
            //        for(Query q: queryList){
            //            q.updateField(0,0,1);
            ////            q.parse("SELECT B, SUM(1), SUM(A), SUM(C), SUM(D), SUM(E) FROM R GROUP BY B;");
            //            q.printResult();
            //            q.printQueryParser();
            //        }
        }
    }

    private static void inputFileNames(int start, int end){
        while(start <= end){
            FILENAMES.add("dataset/sf"+start+".csv");
            start++;
        }
    }

}



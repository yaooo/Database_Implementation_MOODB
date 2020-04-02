import java.util.*;

public class Main {

    // modify this variable for other csv files
    private final static String FILENAME = "dataset/sf20.csv";
    private static List<String> FILENAMES = new LinkedList<>();
    private final static String[] attributes = new String[]{"A", "B", "C", "D", "E"};


    public static void main(String[] args) {


        inputFileNames(17,20);
//        naiveDBBenchMark();
        System.out.print("\n\n");
        mooDBBenchMark();
    }

    private static void naiveDBBenchMark(){
        for(String f: FILENAMES) {

            System.out.println("EXECUTING FILE: " + f);
            NaiveSchema schema = new NaiveSchema(f, Arrays.asList(attributes));
            NaiveQueryBatch qb = new NaiveQueryBatch(schema);

            // read queries
            ArrayList<String> queries = inputQueries();
            // read queries into query batch before processing further
            qb.readQueries(queries);
//            qb.evaluateIndepently();

            qb.evaluateBatch();
        }
    }
    private static void mooDBBenchMark(){
        // create queries
        ArrayList<String> queries = inputQueries();

        // create the trie and read data from the csv file
        for(String f: FILENAMES) {

            System.out.println("EXECUTING FILE: " + f);
            long c = System.currentTimeMillis();
            Schema schema = new Schema(f, Arrays.asList(attributes));
//            Trie trie = schema.getTrie();
//            trie.displayAll();
            System.out.println("Create trie:"+ (System.currentTimeMillis()-c) +"ms");

            QueryBatch3 qb3 = new QueryBatch3(schema);
            qb3.readQueries(queries);
            qb3.evaluate();

            QueryBatch2 qb2 = new QueryBatch2(schema);
            qb2.readQueries(queries);
            qb2.evaluate();

//            //todo: change it back
            QueryBatch1 qb1 = new QueryBatch1(schema);
            qb1.readQueries(queries);
            qb1.evaluate();

        }
    }

    private static void inputFileNames(int start, int end){
//        while(start <= end){
//            FILENAMES.add("dataset/sf"+start+".csv");
//            start++;
//        }
        FILENAMES.add(FILENAME);
    }

    private static ArrayList<String> inputQueries(){
        ArrayList<String> queries = new ArrayList<>();
        queries.add("SELECT A, SUM(1), SUM(B), SUM(C), SUM(D), SUM(E) FROM R GROUP BY A;");
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
        return queries;
    }

}



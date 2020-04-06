import java.util.*;

public class Main {
    private static List<String> FILENAMES = new LinkedList<>();
    private final static String[] attributes = new String[]{"A", "B", "C", "D", "E"};
    private static String FILE_DIR = "dataset";

    public static void main(String[] args){
        // add .csv file names to the list FILENAMES
        inputFileNames(1,20);

        if(args.length == 1){
            if(args[0].equals("a")) section_3a();
            if(args[0].equals("b")) section_3b();
            if(args[0].equals("c")) section_3c();
            if(args[0].equals("d")) section_3d();
            if(args[0].equals("e")) section_3e();
            if(args[0].equals("p")){
                for (String f : FILENAMES) {
                    System.out.println("EXECUTING FILE: " + f);
                    System.out.println("NaiveDB");
                    naiveDBBenchMark(f, true);
                    System.out.println("MooDB");
                    mooDBBenchMark(f, true);
                }
            }
        }else {
            for (String f : FILENAMES) {
                System.out.println("EXECUTING FILE: " + f);
                System.out.println("NaiveDB");
                naiveDBBenchMark(f, false);
                System.out.println("MooDB");
                mooDBBenchMark(f, false);
            }
        }
    }

    private static void section_3a() {
        System.out.println("Running section 3a");
        for (String f : FILENAMES) {
            System.out.println("Testing file "+f+"...");
            NaiveSchema schema = new NaiveSchema(f, Arrays.asList(attributes));
            NaiveQueryBatch qb = new NaiveQueryBatch(schema);
            // read queries
            ArrayList<String> queries = inputQueries();
            // read queries into query batch before processing further
            qb.readQueries(queries);
            qb.evaluateBatch(5, false);
            qb.evaluateIndependently(5);

            System.out.print("Evaluate only the non-group-by query **");
            qb.readQueries(query_aggs());
            qb.evaluateBatch(5, false);
        }
    }

    private static void section_3b() {
        System.out.println("Running section 3b");
        for (String f : FILENAMES) {
            long c = 0;
            for(int i = 0; i < 5; i++){
                if( i == 1) c = System.currentTimeMillis();
                new Trie(f); // build the trie
            }
            double avg = (System.currentTimeMillis() - c) / (5 - 1.0) / 1000.0;
            System.out.println("File - " + f + ", average time needed to construct Trie: " + avg + "s." );
        }
        for (String f : FILENAMES) {
            long c = 0;
            for(int i = 0; i < 5; i++){
                if( i == 1) c = System.currentTimeMillis();
                new NaiveStorage(f); // load to the main memory
            }
            double avg = (System.currentTimeMillis() - c) / (5 - 1.0) / 1000.0;
            System.out.println("File - " + f + ", average time loading the relation to the main memory: " + avg + "s." );
        }
    }

    private static void section_3c() {
        ArrayList<String> queries = inputQueries();
        System.out.println("Running section 3c");
        for (String f : FILENAMES) {
            System.out.println("Testing file "+f+"...");

            // run in a batch
            Schema schema = new Schema(f, Arrays.asList(attributes));
            QueryBatch1 qb1 = new QueryBatch1(schema);
            qb1.readQueries(queries);
            qb1.evaluate(5, false);

            // run independently
            QueryBatch1 qb11 = new QueryBatch1(schema);
            qb11.readQueries(queries);
            qb11.evaluateIndependently();

            // run only the aggs query
            System.out.print("Evaluate only the non-group-by query **");
            qb1.readQueries(query_aggs());
            qb1.evaluate(5, false);
        }
    }

    private static void section_3d() {
        System.out.println("Running section 3d");
        ArrayList<String> queries = inputQueries();

        for (String f : FILENAMES) {
            System.out.println("Testing file "+f+"...");

            // run in a batch
            Schema schema = new Schema(f, Arrays.asList(attributes));
            QueryBatch2 qb2 = new QueryBatch2(schema);
            qb2.readQueries(queries);
            qb2.evaluate(5, false);

            // run independently
            QueryBatch2 qb22 = new QueryBatch2(schema);
            qb22.readQueries(queries);
            qb22.evaluateIndependently();

            // run only the aggs query
            System.out.print("Evaluate only the non-group-by query **");
            qb2.readQueries(query_aggs());
            qb2.evaluate(5, false);
        }
    }

    private static void section_3e() {
        System.out.println("Running section 3e");
        ArrayList<String> queries = inputQueries();

        for (String f : FILENAMES) {
            System.out.println("Testing file "+f+"...");

            // run in a batch
            Schema schema = new Schema(f, Arrays.asList(attributes));
            QueryBatch3 qb3 = new QueryBatch3(schema);
            qb3.readQueries(queries);
            qb3.evaluate(5, false);

            // run independently
            QueryBatch3 qb33 = new QueryBatch3(schema);
            qb33.readQueries(queries);
            qb33.evaluateIndependently();

            // run only the aggs query
            System.out.print("Evaluate only the non-group-by query **");
            qb3.readQueries(query_aggs());
            qb3.evaluate(5, false);

        }
    }

    /**
     * Read queries, output time taken
     */

    private static void naiveDBBenchMark(String f, boolean ifPrint){
        NaiveSchema schema = new NaiveSchema(f, Arrays.asList(attributes));
        NaiveQueryBatch qb = new NaiveQueryBatch(schema);
        // read queries
        ArrayList<String> queries = inputQueries();
        // read queries into query
        qb.readQueries(queries);
        qb.evaluateBatch(1, ifPrint);
    }

    /**
     * Read queries, output time taken
     */
    private static void mooDBBenchMark(String f, boolean ifPrint){
        // create queries
        ArrayList<String> queries = inputQueries();
        Schema schema = new Schema(f, Arrays.asList(attributes));

        // print out the result produced by MooDB-version 1
        QueryBatch1 qb1 = new QueryBatch1(schema);
        qb1.readQueries(queries);
        qb1.evaluate(1, ifPrint);

        // print out the result produced by MooDB-version 2
        QueryBatch2 qb2 = new QueryBatch2(schema);
        qb2.readQueries(queries);
        qb2.evaluate(1, ifPrint);

        // print out the result produced by MooDB-version 3
        QueryBatch3 qb3 = new QueryBatch3(schema);
        qb3.readQueries(queries);
        qb3.evaluate(1, ifPrint);
    }

    // generate the names of all .csv files
    private static void inputFileNames(int start, int end){
        while(start <= end){
            FILENAMES.add(FILE_DIR + "/sf"+start+".csv");
            start++;
        }
    }

    // input testing queries
    private static ArrayList<String> inputQueries(){
        ArrayList<String> queries = new ArrayList<>();
        queries.add("SELECT SUM(1), SUM(A), SUM(B), SUM(C), SUM(D), SUM(E)," +
                "SUM(A*B), SUM(A*C), SUM(A*D), SUM(A*E)," +
                "SUM(B*C), SUM(B*D), SUM(B*E)," +
                "SUM(C*D), SUM(C*E)," +
                "SUM(D*E) " +
                "FROM R;");
        queries.add("SELECT A, SUM(1), SUM(B), SUM(C), SUM(D), SUM(E) FROM R GROUP BY A;");
        queries.add("SELECT B, SUM(1), SUM(A), SUM(C), SUM(D), SUM(E) FROM R GROUP BY B;");
        queries.add("SELECT C, SUM(1), SUM(A), SUM(B), SUM(D), SUM(E) FROM R GROUP BY C;");
        queries.add("SELECT D, SUM(1), SUM(A), SUM(B), SUM(C), SUM(E) FROM R GROUP BY D;");
        queries.add("SELECT E, SUM(1), SUM(A), SUM(B), SUM(C), SUM(D) FROM R GROUP BY E;");
        return queries;
    }

    // input only the non-group-by query
    private static ArrayList<String> query_aggs(){
        ArrayList<String> queries = new ArrayList<>();
        queries.add(inputQueries().get(0));
        return queries;
    }
}
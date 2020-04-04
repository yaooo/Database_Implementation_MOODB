import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

public class Main {

    // modify this variable for other csv files
    private static List<String> FILENAMES = new LinkedList<>();
    private final static String[] attributes = new String[]{"A", "B", "C", "D", "E"};
    private static String file_dir = "..//dataset";
    static boolean printResult = false;
    static int numRun = 5; // By default, run each experiment 5 times
    private static char section = 'a'; // By default, run section a of the benchmark
    private static boolean runAllSection = false;

    public static void main(String[] args) throws IOException {
        IntStream.range(0, args.length).forEach(i -> {
            if(i == 0) file_dir = args[i];
            if (i == 1 && args[i].toUpperCase().equals("ALL")) runAllSection = true;
            if (i == 1) section = args[i].charAt(0);
            if (i == 2) printResult = Boolean.parseBoolean(args[1]);
        });

        Files.list(Paths.get("."))
                .forEach(System.out::println);

        // read files from dataset/sf1.csv to dataset/sf20.csv
//        inputFileNames(1,20);
//        for(String f: FILENAMES){
//            System.out.println("EXECUTING FILE: " + f);
//            mooDBBenchMark(f);
//            naiveDBBenchMark(f);
//        }
//        System.out.print("\n\n");
    }

//    public static void listFilesForFolder() {
//        final File folder = new File(System.absfile_dir);
//        folder.listFiles();
//        for (final File fileEntry : folder.listFiles()) {
//            System.out.println(fileEntry.getName());
//        }
//    }


    private static void section_a(){

    }

    private static void naiveDBBenchMark(String f){
        NaiveSchema schema = new NaiveSchema(f, Arrays.asList(attributes));
        NaiveQueryBatch qb = new NaiveQueryBatch(schema);

        // read queries
        ArrayList<String> queries = inputQueries();
        // read queries into query batch before processing further
        qb.readQueries(queries);

        qb.evaluateIndependently();
        qb.evaluateBatch();
    }
    private static void mooDBBenchMark(String f){
        // create queries
        ArrayList<String> queries = inputQueries();

        Schema schema = new Schema(f, Arrays.asList(attributes));

        QueryBatch3 qb3 = new QueryBatch3(schema);
        qb3.readQueries(queries);
        qb3.evaluate();

        QueryBatch2 qb2 = new QueryBatch2(schema);
        qb2.readQueries(queries);
        qb2.evaluate();

        QueryBatch1 qb1 = new QueryBatch1(schema);
        qb1.readQueries(queries);
        qb1.evaluate();
    }

    private static void inputFileNames(int start, int end){
        while(start <= end){
            FILENAMES.add("dataset/sf"+start+".csv");
            start++;
        }
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



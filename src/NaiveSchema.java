import java.util.List;
public class NaiveSchema {

    private NaiveStorage naiveDB;
    private List<String> attributeOrder;

    NaiveSchema(String fileName, List<String> attrs){
        attributeOrder = attrs; // eg: <A, B, C, D, E>
        naiveDB = new NaiveStorage(fileName, attributeOrder.size());
    }

    NaiveStorage getNaiveStorage(){return naiveDB;}

    /**
     * Return the index of the attribute in the schema,
     * else return -1
     */
    int fieldIndex(String s){
        return attributeOrder.indexOf(s);
    }
}

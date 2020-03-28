import java.util.List;
public class NaiveSchema {

    private NaiveStorage naiveDB;
    private List<String> attributeOrder;

    NaiveSchema(String fileName, List<String> attrs){
        attributeOrder = attrs; // EX: <A, B, C, D>
        naiveDB = new NaiveStorage(fileName, attributeOrder.size());
    }

    NaiveStorage getNaiveStorage(){return naiveDB;}

    List<String> getAttributeOrder(){
        return attributeOrder;
    }

    /* return -1 if not found*/
    int fieldIndex(String s){
        return attributeOrder.indexOf(s);
    }


}

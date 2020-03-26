import java.util.List;
public class Schema {

    private Trie trie;
    private List<String> attributeOrder;

    Schema(String fileName, List<String> attrs){
        attributeOrder = attrs; // EX: <A, B, C, D>
        trie = new Trie(fileName, attributeOrder.size());
    }


    Trie getTrie(){return trie;}

    List<String> getAttributeOrder(){
        return attributeOrder;
    }

    /* return -1 if not found*/
    int fieldIndex(String s){
        return attributeOrder.indexOf(s);
    }


}

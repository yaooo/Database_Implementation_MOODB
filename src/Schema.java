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


    String getAttrByIndex(int i){
        if(i >= attributeOrder.size()) {
            try {
                throw new IllegalAccessException("Index out of bound");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return attributeOrder.get(i);
    }

    String getLastAttribute(){
        return attributeOrder.get(attributeOrder.size()-1);
    }

    /* return -1 if not found*/
    int fieldIndex(String s){
        return attributeOrder.indexOf(s);
    }

    int comparePriority(String attr1, String attr2){
        int index1 = attributeOrder.indexOf(attr1);
        int index2 = attributeOrder.indexOf(attr2);
        if(index1 == -1 || index2 == -1){
            System.out.print(attr1 + "--"+attr2);
            throw new IllegalArgumentException("Attribute does not exist in the schema");
        }

        return index1 - index2;
    }
}

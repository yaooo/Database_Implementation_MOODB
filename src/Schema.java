import java.util.List;
public class Schema {
    private Trie trie;
    private List<String> attributeOrder; // A list of attrs(eg: <A, B, C, D, E>)

    Schema(String fileName, List<String> attrs){
        attributeOrder = attrs;
        trie = new Trie(fileName, attributeOrder.size());
    }

    /**
     * @return the trie structure
     */
    Trie getTrie(){return trie;}

    /**
     * @return a list of attributes in the schema
     */
    List<String> getAttributeOrder(){
        return attributeOrder;
    }

    /**
     * @param i the index of an attribute in the attribute list
     * @return the attribute given its index in the schema
     */
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

    /**
     * @return the last attribute in the schema
     */
    String getLastAttribute(){
        return attributeOrder.get(attributeOrder.size()-1);
    }

    /**
     * Because the attributes are stored as a list in the schema,
     * this function returns the index of attribute in the list
     **/
    int fieldIndex(String s){
        return attributeOrder.indexOf(s);
    }

    /**
     * Return 0 if either attr1 or attr2 does not exist in the schema
     * Else, return the difference of the indices of attr1 and attr2 in the schema
     */
    int comparePriority(String attr1, String attr2){
        int index1 = attributeOrder.indexOf(attr1);
        int index2 = attributeOrder.indexOf(attr2);
        if(index1 == -1 || index2 == -1){
            return 0;
        }
        return index1 - index2;
    }
}

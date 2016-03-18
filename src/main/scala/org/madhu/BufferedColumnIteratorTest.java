package org.madhu;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

class InternalRow{
    public int val;
    public InternalRow(int i){
        val = i;
    }
}


/**
 * Created by kmadhu on 19/2/16.
 */
class BufferedColumnIterator {
    private InternalRow[] arr;
    private int idx = -1, len = -1;

    public BufferedColumnIterator(Iterator<InternalRow> input){
        setInput(input);
    }

    public void setInput(Iterator<InternalRow> input) {
        LinkedList<InternalRow> currentRows = new LinkedList<InternalRow>();
        System.out.println("In compute ... ");
        while(input.hasNext()) {
            currentRows.add(input.next());
        }
        arr = currentRows.toArray(new InternalRow[currentRows.size()]);
        System.out.println("ll at 1" + currentRows.get(2).val);
    }

}

public class BufferedColumnIteratorTest {

    public static void main(String arg[]){

        InternalRow arr[] = new InternalRow[10];
        for(int i = 0; i<10; i++) arr[i] = new InternalRow(i);

        BufferedColumnIterator b = new BufferedColumnIterator(Arrays.asList(arr).iterator());
        System.out.println("Check");

    }
}

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class MyWritable implements Writable {

    private String df;
    private List<String> values;

    public MyWritable(){}

    public MyWritable(String df, List<String> values){
        this.df = df;
        this.values = values;
    }

    public void write(DataOutput out) throws IOException{ }

    public void readFields(DataInput in)throws IOException{ }

    @Override
    public String toString(){
        StringBuilder str = new StringBuilder("[");
        int tag = 1;

        for(String value: values){
            if(tag == 1){
                tag = 0;
                str.append("\"" + df + "#" + value+ "\"");
            }
            else str.append("," + "\"" + df + "#" + value+ "\"");
        }
        str.append("]},");
        return str.toString();
    }
}

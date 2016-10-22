import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;

public class OneStopFlightMapper extends Mapper<LongWritable, Text, Text, TuplePair> {
  // Indices of join attributes for both relations
  public static final int REL_A_JOINKEY_IND = 18;
  public static final int REL_B_JOINKEY_IND = 17;
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    TuplePair outValue = new TuplePair();
    int joinIndex;
    
    //retrieving the filename for which the input record comes from: orders.txt or customers.txt
    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    
    
    // initializing outevalue.relationName and joinIndex
    if (fileName.equals("A.csv")) {
      outValue.setRelationName("A");
      joinIndex = REL_A_JOINKEY_IND;
    } else {
      outValue.setRelationName("B");
      joinIndex = REL_B_JOINKEY_IND;}
    // outkey= a comma-separated list of values of the join attributes
    // outTuple= a comma-separated lust of values of non-join attributes
    //System.out.println(line);
    StringTokenizer st = new StringTokenizer(line, ",");
    int index = 0;
    String outKey = "", outTuple = "";
    while (st.hasMoreTokens()) {
      String r = st.nextToken();
      //System.out.println(r);
      index++;
      if (index==joinIndex)
        outKey += r;
      else
        outTuple += r + ",";
    }
    // removing the extra "," at the end
    outTuple = outTuple.substring(0, outTuple.length() - 1);
    outValue.setTuple(outTuple);
    //System.out.println(outKey);
    context.write(new Text(outKey), outValue);
  }
}

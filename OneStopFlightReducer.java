import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class OneStopFlightReducer extends Reducer<Text, TuplePair, NullWritable, Text> {

  public void reduce(Text joinfield, Iterable<TuplePair> values, Context context)
      throws IOException, InterruptedException {

    //An array to store the tuples coming from the customer relation
	  ArrayList<String> CRecords = new ArrayList<String>();
	  
	//an array to store the tuples coming from the orders relation
	  ArrayList<String> ORecords = new ArrayList<String>();
	 
   
    //Separating the tuples associated with customers and orders into CRecords and ORecords
    for (TuplePair tp : values) {

      if (tp.getRelationName().equals("A"))
        CRecords.add(tp.getTuple());
      else
        ORecords.add(tp.getTuple());


    }
   //Emitting all the combinations  of the tuples in CRecords and ORecords
    String outValue;
    for (String record1 : CRecords)
      for (String record2 : ORecords) {
    	  String[] R1 = record1.split(",");
    	  String[] R2 = record2.split(",");
    	  //compare carrier
    	  if(R1[8].equals(R2[8]))
    	  {
    		  //compare date
    		  if(R1[0].equals(R2[0]) && R1[1].equals(R2[1]) && R1[1].equals(R2[1]))
    		  {
    			  try
    			  {
    			  int AArr = Integer.parseInt(R1[6]);
    			  int BDep = Integer.parseInt(R2[4]);
    			  
    			  //compare arrival and departure difference
    			  if((BDep-AArr)>0100 && (BDep-AArr)<0500)
    			  {
    				  outValue = R1[9] +"," + R1[9] +"," + R1[16] +","  + joinfield  +"," + joinfield +","  + R2[16] +","  + R1[6] +","  + R2[4];
    			      context.write(NullWritable.get(), new Text(outValue));
    			  }
    			  }
    			  catch(NumberFormatException ex)
    			  {
    				  
    			  }
    		  }
    	  }
        
      }
  }
}

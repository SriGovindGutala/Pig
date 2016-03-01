package com.erp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class GoodBad extends EvalFunc<String>
{
	public String exec(Tuple input) throws IOException 
	{
		if (input == null || input.size() == 0)
            return null;
        try
        {
            boolean res,good,bad; 
  	        bad = false;
  	        good = false;
  	        ArrayList<String> data = new ArrayList<String>();
  	        StringTokenizer itr = new StringTokenizer(input.toString(),","); 
  	        
  	        while (itr.hasMoreTokens())
  	        {
  	        	data.add(itr.nextToken());
  	        }
  	        if(data.size() < 4)
 	        {
 	    	  bad = true;
 	        }
 	        else
 	        {
 	         for(int i=0;i<data.size();i++)
 	         {
 	          if(i==1)
 	          {
 	   	      // if the second column is not integer, it's a bad array
 	 	        res = checkstring(data.get(i));
 	 	        if(res != true)
 	 	        {
 	 	      	  bad = true;
 	 	        }   
 	          } 
 	          else
 	          {
 	           if(data.get(i).isEmpty())
 	           {
 	        	  bad = true;
 	           }
 	          }
 	         }
 	        }
  	      String s = new String();
	      for(int i = 0;i<data.size();i++)
	      {
	    	s = s + data.get(i);  
	      }
	      
 	      if (bad == true)
 	      {	    
 	    	  s= s+"bad";
 	    	  return s;
 	      }
 	      else
 	      {
 	    	 s= s+"good";
 	    	 return s;
 	      }
        }
        catch(Exception e)
        {
            throw new IOException("Caught exception processing input row ", e);
        }
	}

	public static boolean checkstring(String s) 
	{	
	   boolean isInt = false;
	   try 
	   { 
	      Integer.parseInt(s);
	      isInt = true;
	   } 
	   catch (NumberFormatException e) 
	   {
	     isInt = false;
	   }
	  return isInt;
    }
}


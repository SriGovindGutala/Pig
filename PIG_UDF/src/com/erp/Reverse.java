package com.erp;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class Reverse extends EvalFunc<String> 
{
	public String exec(Tuple word) throws IOException {

		
		if(word == null || word.size() == 0)
		{
			return null;
		}
		else
		{
			try 
			{
				String[] tempstr = (String[]) word.get(0);
				int left = 0 , right;
				String finalstr = null;
				right = tempstr.length - 1;
				for(left=0;left<right;left++,right--)
				{
					String temp = tempstr[left];
					tempstr[left] = tempstr[right];
					tempstr[right] = temp;
				}
				for(String c : tempstr)
				{
					finalstr = finalstr + c;
			    }
				return finalstr;
			}
			catch(Exception e)
			{
		         throw new IOException("Caught exception processing input row ", e);
			}
		}
		
	}
	
}

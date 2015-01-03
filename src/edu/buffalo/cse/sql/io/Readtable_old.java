package edu.buffalo.cse.sql.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse.sql.Schema;

public class Readtable_old {
	public void main(String[] args)
	{
		
	};
	public String[][] readfile(String path)
	{
	//final String file = "./src/edu/buffalo/cse/"+"test/r.dat";
	final String file = path;

	List<String> firstColList = new ArrayList<String>();
	List<String> secondColList = new ArrayList<String>();
	
	try {
		BufferedReader bf = new BufferedReader(new FileReader(file));
		
		String content = null;
		
		while((content = bf.readLine()) != null){
			String ary[] = content.trim().split(",");
			
			firstColList.add(ary[0]);
			secondColList.add(ary[1]);
		}
		
		bf.close();
		
	} catch (IOException e) {
		e.printStackTrace();
	}
	
	String[] firstColAry = firstColList.toArray(new String[0]);
	String[] secondColAry = secondColList.toArray(new String[0]);
	
	
//	System.out.println("The item in the array is: ");
//	for(int i = 0; i < firstColAry.length; i++){
//		System.out.println(firstColAry[i] + "\t" + secondColAry[i]);
//		
//	//  Map<String, Schema.TableFromFile> map
//	//     = new HashMap<String, Schema.TableFromFile>();
//
//		
//		
//		//Map <String[], List> map = new HashMap<String[], List>();
//		//List list1 = firstColList;
//		//List list2 = secondColList;	
//		//map.put(firstColAry, list1);		
//	//	map.put(secondColAry, list2);
//	//	return map;
//		
//		
//	//return firstColAry[];
//	}
	
      //put firstColAry and secondColAry together 	 
	  String sumColAry[][]=new String[firstColAry.length][2];
		for(int i = 0; i < firstColAry.length; i++)
		{
			for(int j = 0; j < 2; j++)
			{
				if (j==0)
				{
					sumColAry[i][j]=firstColAry[i];
				}
				else
				{
					sumColAry[i][j]=secondColAry[i];
				}
			}
		}
	  
	  //System.arraycopy(firstColAry,0,sumColAry,0,firstColAry.length);
	  //System.arraycopy(secondColAry,0,sumColAry,firstColAry.length,secondColAry.length);
	  
	  return sumColAry;
	
}

}

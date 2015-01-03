package edu.buffalo.cse.sql.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse.sql.Schema;

public class Readtable {
	private int a;
	public void main(String[] args)
	{
		
	};
	public String[][] readfile(String path)
	{
	//final String file = "./src/edu/buffalo/cse/"+"test/r.dat";
	final String file = path;

	List<String> firstColList = new ArrayList<String>();
	List<String> secondColList = new ArrayList<String>();
	String sumColAry1[][]=new String[1][1];
	
	try {
		BufferedReader bf = new BufferedReader(new FileReader(file));
		int l1=1;
		int l2=1;
		
		String content = null;
		content=bf.readLine();
		String ary1[] = content.trim().split("[|]");
		l2=ary1.length;
		while ((content = bf.readLine()) != null){
			l1=l1+1;
			
		}
		bf.close();
		
		BufferedReader bf1 = new BufferedReader(new FileReader(file));
		
		String sumColAry[][]=new String[l1][l2];
		
		l1=0;
		
		while((content = bf1.readLine()) != null){
			String ary[] = content.trim().split("[|]");
			for (int i=0;i<ary.length;i++){
				sumColAry[l1][i]=ary[i];
			}
			l1++;
		}

		bf1.close();

		sumColAry1=sumColAry;
		
	} catch (IOException e) {
		e.printStackTrace();
	}
	
	return sumColAry1;
	
//	String[] firstColAry = firstColList.toArray(new String[0]);
//	String[] secondColAry = secondColList.toArray(new String[0]);
//	
//	
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
//	
//      //put firstColAry and secondColAry together 	 
//	  
//		for(int i = 0; i < firstColAry.length; i++)
//		{
//			for(int j = 0; j < 2; j++)
//			{
//				if (j==0)
//				{
//					sumColAry[i][j]=firstColAry[i];
//				}
//				else
//				{
//					sumColAry[i][j]=secondColAry[i];
//				}
//			}
//		}
	  
	  //System.arraycopy(firstColAry,0,sumColAry,0,firstColAry.length);
	  //System.arraycopy(secondColAry,0,sumColAry,firstColAry.length,secondColAry.length);
	  
	  
	
}

}

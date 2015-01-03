package edu.buffalo.cse.sql.test;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.NullSourceNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.UnionNode;
import edu.buffalo.cse.sql.plan.AggregateNode;
public class Q19 extends TestHarness {
  public static void main(String args[]) {
    TestHarness.main(args, new Q19());
  }
  public void testRA() {
    
  }
  public void testSQL() {
	  
	  try {
		List<List<Datum[]>> computedResults = Sql.execFile(new File("test/TPCH_Q19.SQL"),null);
		//null execquery //-explain (push down, sort merge, index) optimization //-index 
//		int queryID=0;
//		for(; queryID < computedResults.size(); queryID++){
//	        for (Datum[] row : computedResults.get(queryID)){
//	        	for (int i=0;i<row.length;i++){
//	        		System.out.println(row[i].toString());
//	        	}
//	        }
//	                       
//	      }
	} catch (SqlException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
  }
  List<List<Datum[]>> getResults0() {
    List<List<Datum[]>> ret = new ArrayList<List<Datum[]>>();
    ret.add(getResultsUD0());
    return ret;
  }
  ArrayList<Datum[]> getResultsUD0() {
    ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
    ret.add(new Datum[] {new Datum.Int(32)}); 
    return ret;
  }
}

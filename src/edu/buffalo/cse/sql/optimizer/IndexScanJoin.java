package edu.buffalo.cse.sql.optimizer;

import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ScanNode;

public class IndexScanJoin extends PlanRewrite {

	public IndexScanJoin(boolean defaultTopDown) {
		super(defaultTopDown);
		// TODO Auto-generated constructor stub
	}
	public PlanNode apply(PlanNode node) throws SqlException {
		// TODO Auto-generated method stub
		switch(node.type){
		case SCAN:{
			ScanNode scanNode = (ScanNode)node;
			String tablename = scanNode.getTableName();
			if(tablename.equals("LINEITEM") || tablename.equals("lineitem")){
				scanNode.indexscheme = "l.shipdate,l.orderkey,l.partkey";
			}
			if(tablename.equals("ORDERS") || tablename.equals("orders")){
				scanNode.indexscheme ="o.orderdate";
			}
			if(tablename.equals("CUSTOMER") || tablename.equals("customer")){
				scanNode.indexscheme ="c.custkey";
			}
			if(tablename.equals("NATION" )|| tablename.equals("nation")){
				scanNode.indexscheme ="n.nationkey";
			}
			if(tablename.equals("PART")|| tablename.equals("part")){
				scanNode.indexscheme ="p.partkey";
			}
			if(tablename.equals("SUPPLIER")  || tablename.equals("supplier") ){
				scanNode.indexscheme ="s.suppkey";
			}
			if(tablename.equals("REGION") || tablename.equals("region") ){
				scanNode.indexscheme ="r.regionkey";
			}
			return scanNode;
		}
		case JOIN:{
			JoinNode joinNode= (JoinNode)node;
			PlanNode leftchild = joinNode.getLHS();
			PlanNode rightchild = joinNode.getRHS();
			if(leftchild.type == PlanNode.Type.JOIN && rightchild.type == PlanNode.Type.SCAN){
				ScanNode rightscan = (ScanNode)rightchild;
				if(rightscan.condition==null){
					joinNode.setJoinType(JoinNode.JType.INDEX);
				}
			}
			if(leftchild.type == PlanNode.Type.SCAN && rightchild.type == PlanNode.Type.JOIN){
				ScanNode leftscan = (ScanNode)leftchild;
				if(leftscan.condition==null){
					joinNode.setJoinType(JoinNode.JType.INDEX);
				}
			}
			if(leftchild.type == PlanNode.Type.SCAN && rightchild.type == PlanNode.Type.SCAN){
				ScanNode rightscan = (ScanNode)rightchild;
				ScanNode leftscan = (ScanNode)leftchild;
				if(rightscan.condition!=null && leftscan.condition!=null){
					;
				}else{
					joinNode.setJoinType(JoinNode.JType.INDEX);
				}
			}    
			
			return joinNode;
		}
		default:break;
		}
		return node;
	}

}

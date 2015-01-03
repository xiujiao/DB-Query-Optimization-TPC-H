package edu.buffalo.cse.sql.optimizer;

import java.util.Collection;
import java.util.Iterator;

import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;

public class PushDownSelects extends PlanRewrite{

	public PushDownSelects(boolean defaultTopDown) {
		super(defaultTopDown);
		// TODO Auto-generated constructor stub
	}

	@Override
	public PlanNode apply(PlanNode node) throws SqlException {
		// TODO Auto-generated method stub
		switch(node.type)
		{
		case SELECT:{
			SelectionNode selectnode = (SelectionNode)node;
			PlanNode child= selectnode.getChild();
			ExprTree condition=null;
			Collection<ExprTree> conditionclause =  selectnode.conjunctiveClauses();
			Collection<ExprTree> conditionclause2= selectnode.conjunctiveClauses();
//			System.out.println(selectnode.getCondition().toString());
//			System.out.println(conditionclause.toString());
			
			//iterator the conditions Collection to find join qualified predicates
			for(ExprTree conjunctiveCondition: conditionclause){
//				System.out.println(conjunctiveCondition.toString());
				String s_conjunctiveCondition= conjunctiveCondition.toString();
				
				if (s_conjunctiveCondition.matches(".*[AND].*[AND].*[OR].*") || s_conjunctiveCondition.matches("(.*\\..*\\.[a-zA-Z].*)")){
					continue;
				}
				if (s_conjunctiveCondition.matches("(.*\\..*)")){
//					System.out.println(conjunctiveCondition.toString());
			    // find scan , push down predicates
		        switch(child.type){
		        case SCAN:{
		        	ScanNode scanchild = (ScanNode)child;
		        	if(scanchild.getScanCondition()==null){
		        		scanchild.setScanCondition(conjunctiveCondition);
		            }else{
		 				   ExprTree condition2 = new ExprTree(ExprTree.OpCode.AND, scanchild.getScanCondition(),conjunctiveCondition);
		 				  scanchild.setScanCondition(condition2); 
		 			}
		        	break;
		        }
		        case JOIN:{
		        	child = joinupdate((JoinNode)child,conjunctiveCondition);
		        }
		        default: break;
		        }
		        
					// we already put this condition to scan , so delete it in the select
	            for(Iterator<ExprTree> it = conditionclause2.iterator(); it.hasNext();){
	            	if (it.next()== conjunctiveCondition){
	            		it.remove();
	            		break;
	            	}	
	            }
//	            System.out.println(conditionclause2.toString());
				}
				// put condition clause back into condition
				
			}// for conditions
			int count =0;
//			System.out.println(conditionclause2.toString());
			if (conditionclause2.isEmpty()){
				return child;
			}
			for(Iterator<ExprTree> it = conditionclause2.iterator(); it.hasNext();){
				if (count ==0)
				{
					condition = it.next();
//					System.out.println(condition.toString());
					count ++;
					continue;
				}
				ExprTree condition2 = new ExprTree(ExprTree.OpCode.AND, condition,it.next());
				condition = condition2;
//				System.out.println(condition.toString());
			}
			selectnode = SelectionNode.make(child, condition);
			return selectnode;
		}// case select
        default:
        break;
	   }
		return node;
	}



public JoinNode joinupdate(JoinNode node, ExprTree condition)  throws SqlException {
	   PlanNode leftchild =node.getLHS();
	   PlanNode rightchild = node.getRHS();
	   
	   String s_condition = condition.toString();
	   s_condition.trim();
	   String[] temp1 = s_condition.split("\\.");
	   String relationname1 = temp1[0].substring(1,2).toLowerCase();
	  
	   
	   switch(leftchild.type){
	   case SCAN:{
		   ScanNode leftnode = (ScanNode)leftchild;
		   String tablename = leftnode.getTableName();
		   String temp3 = tablename.substring(0, 1).toLowerCase();
		   if(temp3.equals(relationname1) ){
			   if(leftnode.getScanCondition()==null){
				   leftnode.setScanCondition(condition);}else{
				   ExprTree condition2 = new ExprTree(ExprTree.OpCode.AND, leftnode.getScanCondition(),condition);
				   leftnode.setScanCondition(condition2); 
			   }
			   node.setLHS(leftnode);
			   return node;
		   }else{
			   switch(rightchild.type){
			   case SCAN:{
				   ScanNode rightnode = (ScanNode)rightchild;
				   if(rightnode.getScanCondition()==null){
					   rightnode.setScanCondition(condition);}else{
						   ExprTree condition2 = new ExprTree(ExprTree.OpCode.AND, rightnode.getScanCondition(),condition);
						   rightnode.setScanCondition(condition2); 
					   }
				   node.setRHS(rightnode);	 
				   break;
			   }
			   case JOIN:{
				   node.setRHS(joinupdate((JoinNode)rightchild,condition));
			   }
			   default: break;
			   }
			   return node;
		   }
		   
	   }
	   default: break;
	   }
	   
	   switch(rightchild.type){
	   case SCAN:{
		   ScanNode rightnode = (ScanNode)rightchild;
		   String tablename = rightnode.getTableName();
		   String temp3 = tablename.substring(0, 1).toLowerCase();
		   
		   if(temp3.equals(relationname1)){
			   if(rightnode.getScanCondition()==null){
				   rightnode.setScanCondition(condition);}else{
					   ExprTree condition2 = new ExprTree(ExprTree.OpCode.AND, rightnode.getScanCondition(),condition);
					   rightnode.setScanCondition(condition2); 
				   }
			   node.setRHS(rightnode);
			   return node;
		   }else{
			   
			   switch(leftchild.type){
			   case SCAN:{
				   ScanNode leftnode = (ScanNode)leftchild;
				   if(leftnode.getScanCondition()==null){
					   leftnode.setScanCondition(condition);}else{
						   ExprTree condition2 = new ExprTree(ExprTree.OpCode.AND, leftnode.getScanCondition(),condition);
						   leftnode.setScanCondition(condition2); 
					   }
				   node.setLHS(leftnode);	   
				   break;
			   }
			   case JOIN:{
				   node.setLHS(joinupdate((JoinNode)leftchild,condition));
				   break;
			   }
			   default: break;
			   }
		    
			   return node;
		   }
		   
	   }
	   default: break;
	   }
	  
		return node;
	}
}
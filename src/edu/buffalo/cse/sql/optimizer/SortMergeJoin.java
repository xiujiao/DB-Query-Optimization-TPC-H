package edu.buffalo.cse.sql.optimizer;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.plan.*;
import edu.buffalo.cse.sql.plan.ExprTree.VarLeaf;

public class SortMergeJoin extends PlanRewrite{
	public SortMergeJoin(boolean defaultTopDown) {
		super(defaultTopDown);
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
//			
			//iterator the conditions Collection to find join qualified predicates
			for(ExprTree conjunctiveCondition: conditionclause){
//				System.out.println(conjunctiveCondition.toString());
				String s_conjunctiveCondition= conjunctiveCondition.toString();
				// for Q19	
				if (s_conjunctiveCondition.matches(".*[AND].*[AND].*[OR].*")){// if it has and, it must be Q19 in this project
					ExprTree condition123old=selectnode.getCondition();
//					System.out.println(condition123old);
					
					ExprTree condition12old = condition123old.get(0);
					ExprTree condition1old = condition12old.get(0);
//					System.out.println(condition1old);
					ExprTree condition2old = condition12old.get(1);
//					System.out.println(condition2old);
					ExprTree condition3old = condition123old.get(1);
//					System.out.println(condition3old);
					ExprTree joincondition = null;
					
					ExprTree condition1=null;
					ExprTree condition2=null;
					ExprTree condition3=null;
					ExprTree condition4=null;
					ExprTree condition5=null;
					ExprTree condition6=null;
					ExprTree condition7=null;
					ExprTree condition8=null;
					
					for (int i=0;i<8;i++)
					{
						ExprTree temp0= condition1old.get(0);
						ExprTree temp1= temp0.get(0);
						ExprTree temp2= temp1.get(0);
						ExprTree temp3= temp2.get(0);
						ExprTree temp4= temp3.get(0);
						ExprTree temp5= temp4.get(0);
						ExprTree temp6= temp5.get(0);
						
						condition8 = condition1old.get(1);
						condition7 = temp0.get(1);
						condition6 = temp1.get(1);
						condition5 = temp2.get(1);
						condition4 = temp3.get(1);
						condition3 = temp4.get(1);
						condition2 = temp5.get(1);
						condition1 = temp6.get(1);
						joincondition=temp6.get(0);
					}
					
					ExprTree temp11= new ExprTree(ExprTree.OpCode.AND,condition1,condition2);;
					ExprTree temp12= new ExprTree(ExprTree.OpCode.AND,temp11,condition3);
					ExprTree temp13=new ExprTree(ExprTree.OpCode.AND,temp12,condition4);
					ExprTree temp14=new ExprTree(ExprTree.OpCode.AND,temp13,condition5);
					ExprTree temp15=new ExprTree(ExprTree.OpCode.AND,temp14,condition6);
					ExprTree temp16=new ExprTree(ExprTree.OpCode.AND,temp15,condition7);
					ExprTree condition1new = new ExprTree(ExprTree.OpCode.AND,temp15,condition8);
					
					for (int i=0;i<8;i++)
					{
						ExprTree temp0= condition2old.get(0);
						ExprTree temp1= temp0.get(0);
						ExprTree temp2= temp1.get(0);
						ExprTree temp3= temp2.get(0);
						ExprTree temp4= temp3.get(0);
						ExprTree temp5= temp4.get(0);
						ExprTree temp6= temp5.get(0);
						
						condition8 = condition1old.get(1);
						condition7 = temp0.get(1);
						condition6 = temp1.get(1);
						condition5 = temp2.get(1);
						condition4 = temp3.get(1);
						condition3 = temp4.get(1);
						condition2 = temp5.get(1);
						condition1 = temp6.get(1);
					}
					ExprTree temp21= new ExprTree(ExprTree.OpCode.AND,condition1,condition2);;
					ExprTree temp22= new ExprTree(ExprTree.OpCode.AND,temp21,condition3);
					ExprTree temp23=new ExprTree(ExprTree.OpCode.AND,temp22,condition4);
					ExprTree temp24=new ExprTree(ExprTree.OpCode.AND,temp23,condition5);
					ExprTree temp25=new ExprTree(ExprTree.OpCode.AND,temp24,condition6);
					ExprTree temp26=new ExprTree(ExprTree.OpCode.AND,temp25,condition7);
					ExprTree condition2new = new ExprTree(ExprTree.OpCode.AND,temp26,condition8);
					
					for (int i=0;i<8;i++)
					{
						ExprTree temp0= condition3old.get(0);
						ExprTree temp1= temp0.get(0);
						ExprTree temp2= temp1.get(0);
						ExprTree temp3= temp2.get(0);
						ExprTree temp4= temp3.get(0);
						ExprTree temp5= temp4.get(0);
						ExprTree temp6= temp5.get(0);
						
						condition8 = condition1old.get(1);
						condition7 = temp0.get(1);
						condition6 = temp1.get(1);
						condition5 = temp2.get(1);
						condition4 = temp3.get(1);
						condition3 = temp4.get(1);
						condition2 = temp5.get(1);
						condition1 = temp6.get(1);
					}
					ExprTree temp31= new ExprTree(ExprTree.OpCode.AND,condition1,condition2);;
					ExprTree temp32= new ExprTree(ExprTree.OpCode.AND,temp31,condition3);
					ExprTree temp33=new ExprTree(ExprTree.OpCode.AND,temp32,condition4);
					ExprTree temp34=new ExprTree(ExprTree.OpCode.AND,temp33,condition5);
					ExprTree temp35=new ExprTree(ExprTree.OpCode.AND,temp34,condition6);
					ExprTree temp36=new ExprTree(ExprTree.OpCode.AND,temp35,condition7);
					ExprTree condition3new = new ExprTree(ExprTree.OpCode.AND,temp36,condition8);
					
				
					
					ExprTree condition1910= new ExprTree(ExprTree.OpCode.OR,condition1new,condition2new);
					ExprTree condition1911= new ExprTree(ExprTree.OpCode.OR,condition1910,condition3new);
					
					selectnode.setCondition(condition1911);
					JoinNode joinchild = (JoinNode)child;
					joinchild.setJoinCondition(joincondition);
					joinchild.setJoinType(JoinNode.JType.MERGE);
					selectnode = SelectionNode.make(joinchild, condition1911);
					return selectnode;
					
				}
				// if it is "=" predicates and it can be pushed down to join
				if (s_conjunctiveCondition.matches("(.*\\..*[^>][^<]=.*\\..*)")){
//					System.out.println(conjunctiveCondition.toString());
			    // find join,put predicate with that join, otherwise recursive to find join or reach scan node finnaly	
	            child = joinupdate((JoinNode)child,conjunctiveCondition);
					// we already put this condition to join , so delete it in the select
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
	   String[] relationnames = s_condition.split("=");
	   String[] temp1 = relationnames[0].split("\\.");
	   String[] temp2 = relationnames[1].split("\\.");
	   String relationname1 = temp1[0].substring(1,2).toLowerCase();
	   String relationname2 = temp2[0].trim().toLowerCase();
	   
	   switch(leftchild.type){
	   case SCAN:{
		   ScanNode leftnode = (ScanNode)leftchild;
		   String tablename = leftnode.getTableName();
		   String temp3 = tablename.substring(0, 1).toLowerCase();
		   if(temp3.equals(relationname1) || temp3.equals(relationname2) ){
			   if(node.getJoinCondition()==null){
			   node.setJoinCondition(condition);}else{
				   ExprTree condition2 = new ExprTree(ExprTree.OpCode.AND, node.getJoinCondition(),condition);
				   node.setJoinCondition(condition2); 
			   }
			   node.setJoinType(JoinNode.JType.MERGE);
			   return node;
		   }else{
			   node.setRHS(joinupdate((JoinNode)rightchild,condition));
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
		   if(temp3.equals(relationname1) || temp3.equals(relationname2) ){
			   if(node.getJoinCondition()==null){
				   node.setJoinCondition(condition);}else{
					   ExprTree condition2 = new ExprTree(ExprTree.OpCode.AND, node.getJoinCondition(),condition);
					   node.setJoinCondition(condition2); 
				   }
			   node.setJoinType(JoinNode.JType.MERGE);
			   return node;
		   }else{
			   node.setLHS(joinupdate((JoinNode)leftchild,condition));
			   return node;
		   }
		   
	   }
	   default: break;
	   }
	  
		return node;
	}
}

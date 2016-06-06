package queryTranslator.sparql;

import java.util.ArrayList;

import javax.print.attribute.standard.Chromaticity;

import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.ontology.OntTools.Path;
import com.hp.hpl.jena.sparql.algebra.op.OpPath;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.path.PathFactory;
import com.hp.hpl.jena.sparql.pfunction.library.container;
/**
 * 
 * @author Simon Skilevic
 *
 */
public class TransformerHelper {
	private static int propartyPathVariableCounter = 0;

	private static boolean checkInverse(String input){
		if (input.charAt(0)=='^')
			if (input.charAt(1)=='('){
				int count=1;
				int id=2;
				while(id < input.length() && count != 0){
					if (input.charAt(id)=='(')
						count++;
					if (input.charAt(id)==')')
						count--;
					id++;
				}
				if (count == 0 && id == input.length())
					return true;
				else return false;
			} else if (getPosOfNextPossibleOperator(input, 0)<0)
				return true;
			else return false;
				
		else 
			return false;
	}
	
	public static ArrayList<String> findAllPossiblePathes(String sPath){
		ArrayList<String> res = new ArrayList<String>();
		boolean inverse = checkInverse(sPath);
		if (inverse)
		{
			sPath = sPath.substring(1);
		}
		sPath = TransformerHelper.removeScopes(sPath);
		int operatorPos = TransformerHelper.getPosOfNextOperator(sPath);
		if (operatorPos < 0)
		{
			if (inverse)
				res.add("^"+sPath);
			else
				res.add(sPath);
			return res;
		} else if (sPath.charAt(operatorPos)=='/'){
			ArrayList<String> left = findAllPossiblePathes(sPath.substring(0,operatorPos));
			ArrayList<String> right = findAllPossiblePathes(sPath.substring(operatorPos+1));
			for (int i=0; i < left.size();i++)
				for (int j=0; j < right.size();j++) {
					String newPath ="";
					if (inverse) newPath+="^";
					newPath += "("+left.get(i)+"/"+right.get(j)+")";
					res.add(newPath);
				}
		} else if (sPath.charAt(operatorPos)=='|'){
			ArrayList<String> left = findAllPossiblePathes(sPath.substring(0,operatorPos));
			ArrayList<String> right = findAllPossiblePathes(sPath.substring(operatorPos+1));
			for (int i=0; i < left.size();i++){
				String newPath ="";
				if (inverse) newPath+="^";
				newPath+=left.get(i);
				res.add(newPath);
			}
			for (int j=0; j < right.size();j++) {
				String newPath ="";
				if (inverse) newPath+="^";
				newPath+=right.get(j);
				res.add(newPath);
			}
		}
		return res;
	}
	
	public static BasicPattern transformPathToBasicPattern(Node subject, String sPath, Node object){
		BasicPattern res= new BasicPattern();
		boolean inverse = checkInverse(sPath);
		if (inverse)
		{
			sPath = sPath.substring(1);
			Node temp = subject;
			subject = object;
			object = temp;
		}
		sPath = TransformerHelper.removeScopes(sPath);
		int operatorPos = TransformerHelper.getPosOfNextOperator(sPath);
		if (operatorPos < 0)
		{
			Triple triple = new Triple(subject, NodeFactory.createURI(sPath.substring(1, sPath.length()-1)), object);
			res.add(triple);
			return res;
		}
    	String leftStringPath = sPath.substring(0, operatorPos);
    	String rightStringPath = sPath.substring(operatorPos+1);
    	Node newConection = NodeFactory.createVariable(getNextVaribleName());
    	BasicPattern leftPattern = transformPathToBasicPattern(subject, leftStringPath, newConection);
    	BasicPattern rightPattern = transformPathToBasicPattern(newConection, rightStringPath, object);
    	res.addAll(leftPattern);
    	res.addAll(rightPattern);
		return res;
	}
	
	public static int getPosOfNextOperator(String input){
		int i = getPosOfNextPossibleOperator(input, 0);
		while(i>=0) {
			int left = 0;
			int right = 0;
			for (int jL=i-1; jL>=0; jL--){
				if (input.charAt(jL)==')')
					left++;
				else if (input.charAt(jL)=='(')
					left--;
			}
			if (left != 0) {
				i = getPosOfNextPossibleOperator(input, i+1);
				continue;
			}
			
			for (int jR=i+1; jR<input.length(); jR++){
				if (input.charAt(jR)=='(')
					right++;
				else if (input.charAt(jR)==')')
					right--;
			}
			if (right == 0) return i;
			i = getPosOfNextPossibleOperator(input, i+1);
		}
		return -1;
	}
	
	private static int getPosOfNextPossibleOperator(String input, int begin){
		for (int i=begin; i < input.length(); i++){
			if (input.charAt(i)=='/' || input.charAt(i)=='|'){
				return i;
				
			} else if (input.charAt(i)=='<') // skip uri
				while (input.charAt(i)!='>') i++;
			else if (input.charAt(i)=='"'){ // skip literal
				i++;
				while (input.charAt(i)!='"') i++;
			}
		}
		return -1;
	}
	
	public static String removeScopes(String input){
		if (input.charAt(0)=='(' && input.charAt(input.length()-1)==')')
			return input.substring(1, input.length()-1);
		return input;
	}

	public static String getNextVaribleName(){
		return "tX"+propartyPathVariableCounter++;
	}

}

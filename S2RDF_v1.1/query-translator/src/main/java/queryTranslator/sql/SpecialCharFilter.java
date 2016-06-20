package queryTranslator.sql;

public class SpecialCharFilter {

	public static String filter(String s){
		s = s.replace(':', '_');
		s = s.replace('-', '_');
		s = s.replace('<', '_');
		s = s.replace('>', '_');
		s = s.replace('/', '_');
		s = s.replace('#', '_');
		s = s.replace('~', '_');
		s = s.replace('.', '_');
		if (s.charAt(0) == '_')
				s = s.substring(1);
		return s;
	}
	
	
}

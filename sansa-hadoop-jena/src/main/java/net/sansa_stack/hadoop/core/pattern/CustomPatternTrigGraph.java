package net.sansa_stack.hadoop.core.pattern;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

public class CustomPatternTrigGraph
	implements CustomPattern
{
	// Pattern for reverse matching strings such as " graph? <foo>"
	// 
	public static Pattern TRIG_GRAPH_REVERSE_PATTERN = Pattern.compile("\\s*((>[^<]<\\s*)|[^\\s]+:\\w*)(hparg)?", Pattern.CASE_INSENSITIVE); 

	@Override
	public CustomMatcher matcher(CharSequence charSequence) {
        return new CustomMatcherTrigGraph(charSequence);
	}

	
	public static class CustomMatcherTrigGraph
		extends CustomMatcherBase {

		protected int lastMatchPosStart = -1;
		protected int lastMatchPosEnd = -1;
		
		public CustomMatcherTrigGraph(CharSequence charSequence) {
			super(charSequence);
		}

		@Override
		public boolean find() {
			boolean result = false;
	        if (pos >= regionStart) {
		        while (true) {
		        	if (pos >= regionEnd) {
		        		break;
		        	}
		        	
			        char c = charSequence.charAt(pos);		        
			        if (c == '{') {
			        	// Check for a preceding IRI
			        	CharSequence reverse = new CharSequenceReverse(charSequence, pos);	        	
			        	Matcher m = TRIG_GRAPH_REVERSE_PATTERN.matcher(reverse);
			        	if (m.find()) {
			        		result = true;
			        		int end = m.end();
			        		lastMatchPosStart = pos - end;
			        		lastMatchPosEnd = pos;
					        ++pos; // Increment for next iteration
					        break;
			        	}
			        }
			        ++pos;
		        }  
	        }
	        return result;
		}

		@Override
		public int start() {
			return lastMatchPosStart;
		}

		@Override
		public int end() {
			return lastMatchPosEnd;
		}

		@Override
		public String group() {
			if (lastMatchPosStart < 0) {
				throw new IllegalStateException();
			}
			
			String result = charSequence.subSequence(lastMatchPosStart, lastMatchPosEnd).toString();
			return result;
		}		
	}
}

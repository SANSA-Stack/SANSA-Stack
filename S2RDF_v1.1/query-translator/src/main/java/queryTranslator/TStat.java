/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package queryTranslator;

/**
 * Small statistic class, which objects contain statistic information about
 * some VP/ExtVP tables    
 * @author Simon Skilevic
 *
 */
public class TStat {
	// VP/ExtVP table size 
	public long size = 0;
	// size of the "parent" VP/TT table corresponding to the ExtVP/VP table
	// described by TStat object
	public long sizeSource = 0;
	// scale of the table 
	// |ExtVP|/|parent VP| for ExtVP table
	// |VP|/|parent TT| for VP table
	public float scale = 0;
	
	public TStat(String size, String sizeSource, String freq){
		this.size = Long.parseLong(size);
		this.sizeSource = Long.parseLong(sizeSource);
		this.scale = Float.parseFloat(freq);
	}

}

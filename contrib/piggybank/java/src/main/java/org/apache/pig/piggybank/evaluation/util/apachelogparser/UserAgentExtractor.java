package org.apache.pig.piggybank.evaluation.util.apachelogparser;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

public class UserAgentExtractor extends EvalFunc<String> {
	
	public String exec(Tuple tuple) { //throws IOException {
	    if (tuple == null || tuple.size() < 1) {
	      return null;
	    }
	    try {
	      String refAgent = (String) tuple.get(0);
	      return deviceString(refAgent);
	    } catch (Exception e) {
	      //catch (ExecException ee) {
	      //throw new IOException(ee);
	      return "ERROR PARSING USER DEVICE";
	    }
	  }
	
	  /*
	   * This method has been added to piggybank to evaluate the user agent to parse the device type.
	  */
	  private String deviceString(String refAgent) {
		  try
		  {
			  UserAgentStringParser parser = UADetectorServiceFactory.getCachingAndUpdatingParser();
			  ReadableUserAgent rAgent = parser.parse(refAgent);
			  return rAgent.getDeviceCategory().getName();
		  } catch (Exception ignore) {
			  return null;
		  }
	  }
}

package org.apache.pig.piggybank.evaluation.string;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;

// Hive already has a ParseUrl UDF that has more functionality than this. If those features are needed, this code should just use the hive UDF code.
// In the hive source, it's in ql/src/java/org/apache/hadoop/hive/ql/udf/UDFParseUrl.java
// They use URLs instead of URIs. I'm not sure what consequences that has.

public class ParseUrl extends EvalFunc<String> {
    
	@Override
	public String exec(Tuple input) {

		if (input == null || input.size() == 0) {
			return null;
		}

		String inputURI;
		String component;

		try {
			inputURI = (String) input.get(0);

			if (inputURI == null)
				return null;

			component = (String) input.get(1);

		} catch (ExecException e) {
			warn("Error processing arguments to ParseURL()", PigWarning.UDF_WARNING_1);
			return null;
		}

		URI uri;

		try {
			uri = new URI(inputURI);
		} catch (URISyntaxException e) {
			warn("Error converting input to URI", PigWarning.UDF_WARNING_1);
			return null;
		}

		if (component.equals("PATH")) {
			return uri.getPath();
		}

		return null;
    }
}

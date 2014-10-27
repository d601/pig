package org.apache.pig.piggybank.evaluation.string;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;

import org.apache.commons.lang.StringUtils;

public class LeftPad extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
		try {
			String string = (String) input.get(0);
			//int size = Integer.parseInt((String) input.get(1));
			// This is ugly as shit but you can't cast an object directly to an int (?)
			int size = ((Integer) input.get(1)).intValue();
			char padChar = ((String) input.get(2)).charAt(0);

			return StringUtils.leftPad(string, size, padChar); 
		} catch (ExecException e) {
			warn("Error processing row in LeftPad()", PigWarning.UDF_WARNING_1);
			return null;
		}
	}
}

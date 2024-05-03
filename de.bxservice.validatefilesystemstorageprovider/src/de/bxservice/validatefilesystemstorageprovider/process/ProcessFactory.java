package de.bxservice.validatefilesystemstorageprovider.process;

import org.adempiere.base.IProcessFactory;
import org.compiere.process.ProcessCall;

public class ProcessFactory implements IProcessFactory {

	@Override
	public ProcessCall newProcessInstance(String className) {
		if (CheckFiles.class.getName().equals(className))
			return new CheckFiles();

		return null;
	}

}

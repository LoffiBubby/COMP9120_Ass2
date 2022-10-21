package Business;

import java.util.Vector;

import Data.RepositoryProviderFactory;

/**
 * Encapsulates any business logic to be executed on the app server; 
 * and uses the data layer for data queries/creates/updates/deletes
 * @author IwanB
 *
 */
public class InstructionProvider implements IInstructionProvider{

	/**
	 * Check login credentials
	 * @param userName : the userName of user credentials
	 * @param password : the password of user credentials
	 */
	@Override
	public String checkAdmCredentials(String userName, String password) {
		return RepositoryProviderFactory.getInstance().getRepositoryProvider().checkAdmCredentials(userName, password);
	}

	/**
	 * Update the details for a given instruction
	 * @param instruction : the instruction for which to update details
	 */
	@Override
	public void updateInstruction(Instruction instruction) {
		RepositoryProviderFactory.getInstance().getRepositoryProvider().updateInstruction(instruction);
	}

	/**
	 * Find instructions associated in some way with a userName
	 * Instructions which have the parameter below should be included in the result
	 * @param id
	 * @return
	 */
	@Override
	public Vector<Instruction> findInstructionsByAdm(String userName) {
		return RepositoryProviderFactory.getInstance().getRepositoryProvider().findInstructionsByAdm(userName);
	}
	
	/**
	 * Add the details for a new instruction to the database
	 * @param instruction : the new instruction to add
	 */
	@Override
	public void addInstruction(Instruction instruction) {
		RepositoryProviderFactory.getInstance().getRepositoryProvider().addInstruction(instruction);
	}

	/**
	 * Given an expression searchString like 'word' or 'this phrase', this method should return 
	 * any instructions that contains this phrase.
	 * @param searchString: the searchString to use for finding instructions in the database
	 * @return
	 */
	@Override
	public Vector<Instruction> findInstructionsByCriteria(String searchString) {
		return RepositoryProviderFactory.getInstance().getRepositoryProvider().findInstructionsByCriteria(searchString);

	}

}

package Business;

import java.util.Vector;

/**
 * Encapsulates any business logic to be executed on the app server; 
 * and uses the data layer for data queries/creates/updates/deletes
 * @author IwanB
 *
 */
public interface IInstructionProvider {

	/**
	 * Check login credentials
	 * @param userName : the userName of user credentials
	 * @param password : the password of user credentials
	 */
	public String checkAdmCredentials(String userName, String password);
	
	/**
	 * Find instructions associated in some way with a userName
	 * Instructions which have the parameter below should be included in the result
	 * @param id
	 * @return
	 */
	public Vector<Instruction> findInstructionsByAdm(String userName);
	
	/**
	 * Given an expression searchString like 'word' or 'this phrase', this method should return 
	 * any instructions that contains this phrase.
	 * @param searchString : the searchString to use for finding instructions in the database
	 * @return
	 */
	public Vector<Instruction> findInstructionsByCriteria(String searchString);	
	
	/**
	 * Add the details for a new instruction to the database
	 * @param instruction : the new instruction to add
	 */
	public void addInstruction(Instruction instruction);

	/**
	 * Update the details for a given instruction
	 * @param instruction : the instruction for which to update details
	 */
	public void updateInstruction(Instruction instruction);
}

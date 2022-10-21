package Presentation;

import java.util.Vector;

import Business.Instruction;

/**
 * Encapsulates create/read/update/delete operations to database
 * @author IwanB
 *
 */
public interface IRepositoryProvider {
	/**
	 * Check login credentials
	 * @param userName: the userName of user credentials
	 * @param password: the password of user credentials
	 */
	public String checkAdmCredentials(String userName, String password);
	
	/**
	 * Find associated instructions given an employee userName
	 * @param user: the employee userName
	 * @return
	 */
	public Vector<Instruction> findInstructionsByAdm(String userName);
	
	/**
	 * Find the associated instructions based on the searchString provided as the parameter
	 * @param searchString: see assignment description search specification
	 * @return
	 */
	public Vector<Instruction> findInstructionsByCriteria(String searchString);	
	
	/**
	 * Add a new instruction into the database
	 * @param instruction: the new instruction to add
	 */
	public void addInstruction(Instruction instruction);

	/**
	 * Update the details for a given instruction
	 * @param instruction: the instruction for which to update details
	 */
	public void updateInstruction(Instruction instruction);
}

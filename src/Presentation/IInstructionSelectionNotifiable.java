package Presentation;

import Business.Instruction;

/**
 * 
 * @author IwanB
 * 
 * Used to notify any interested object that implements this interface
 * and registers with InstructionListPanel of an InstructionSelection
 *
 */
public interface IInstructionSelectionNotifiable {
	public void instructionSelected(Instruction instruction);
}

package Presentation;

import java.awt.BorderLayout;

import javax.swing.JDialog;

import Business.Instruction;

/**
 * 
 * @author IwanB
 *
 * AddEventDialog - used to add a new event
 * 
 */
public class AddInstructionDialog extends JDialog{

	/**
	 * 
	 */
	private static final long serialVersionUID = 173323780409671768L;
	
	/**
	 * detailPanel: reuse EventDetailPanel to add events
	 */
	private InstructionDetailPanel detailPanel = new InstructionDetailPanel(false);

	public AddInstructionDialog()
	{
		setTitle(StringResources.getAppTitle());
		detailPanel.initInstructionDetails(getBlankInstruction());
		add(detailPanel);
		updateLayout();
		setSize(400, 400);
	}

	private void updateLayout() {
		BorderLayout layout = new BorderLayout();
		setLayout(layout);
		layout.addLayoutComponent(detailPanel, BorderLayout.CENTER);
	}

	private Instruction getBlankInstruction() {
		Instruction instruction = new Instruction();
		return instruction;
	}
}

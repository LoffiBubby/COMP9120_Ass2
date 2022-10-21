package Presentation;

import java.awt.BorderLayout;
import java.awt.Color;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import Business.Instruction;

/**
 * Panel encapsulating instruction list
 * @author IwanB
 *
 */
public class InstructionListPanel extends JPanel {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1013855025757989473L;
	
	private List<IInstructionSelectionNotifiable> notifiables = new ArrayList<IInstructionSelectionNotifiable>();
	private Vector<Instruction> instructions;
	
	/**
	 * 
	 * @param events vector of events to display in the event list panel
	 */
	public InstructionListPanel(Vector<Instruction> instructions)
	{
		this.instructions = instructions;
		this.setBorder(BorderFactory.createLineBorder(Color.black));	
		initList(this.instructions);
	}

	private void initList(Vector<Instruction> instructions) {
		
		final JList<Instruction> list = new JList<Instruction>(instructions);
		list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		list.setLayoutOrientation(JList.VERTICAL);
		JScrollPane listScroller = new JScrollPane(list);
		this.add(listScroller);
		
		BorderLayout listLayout = new BorderLayout();
		listLayout.addLayoutComponent(listScroller, BorderLayout.CENTER);
		this.setLayout(listLayout);
		list.getSelectionModel().addListSelectionListener(new ListSelectionListener()
		{
			@Override
			public void valueChanged(ListSelectionEvent e) {
				for(IInstructionSelectionNotifiable notifiable : notifiables)
				{
					Instruction selectedInstruction = list.getSelectedValue();
					if(selectedInstruction != null)
					{
						notifiable.instructionSelected(selectedInstruction);
					}
				}
			}		
		});
	}
	
	/**
	 * Refresh instruction list to display vector of event objects
	 * @param instructions - vector of instruction objects to display
	 */
	public void refresh(Vector<Instruction> instructions)
	{
		this.removeAll();
		this.initList(instructions);
		this.updateUI();
		this.notifiables.clear();
	}
	
	/**
	 * Register an object to be notified of a event selection change
	 * @param notifiable object to invoke when a new event is selected
	 */
	public void registerEventSelectionNotifiableObject(IInstructionSelectionNotifiable notifiable)
	{
		notifiables.add(notifiable);
	}

}

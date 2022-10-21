package Presentation;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import Business.BusinessComponentFactory;
import Business.Instruction;


/**
 * 
 * @author IwanB
 * Panel used for creating and updating instructions
 */
public class InstructionDetailPanel extends JPanel implements IInstructionSelectionNotifiable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2031054367491790942L;

	private Instruction currentInstruction = null;
	private boolean isUpdatePanel = true;

	private JTextField instructionIdField;
	private JTextField amountField;
	private JTextField frequencyField;
	private JTextField expiryDateField;
	private JTextField customerField;
	private JTextField etfField;
	private JTextArea notesArea;
	
	/**
	 * Panel used for creating and updating instructions
	 * @param isUpdatePanel : describes whether panel will be used to either create or update instruction
	 */
	public InstructionDetailPanel(boolean isUpdatePanel)
	{
		this.setBorder(BorderFactory.createLineBorder(Color.black));
		this.isUpdatePanel = isUpdatePanel;
	}

	/**
	 * Re-populates panel details with given instruction object
	 * @param instruction new instruction object to populate panel details with
	 */
	public void initInstructionDetails(Instruction instruction) {
		removeAll();	
		if(instruction != null)
		{
			currentInstruction = instruction;
			addAll();
		}
	}

	private void addAll() {
		JPanel lTextFieldPanel = new JPanel();
		BoxLayout lTextFieldLayout = new BoxLayout(lTextFieldPanel, BoxLayout.Y_AXIS);
		lTextFieldPanel.setLayout(lTextFieldLayout);

		BorderLayout lPanelLayout = new BorderLayout();	
		lPanelLayout.addLayoutComponent(lTextFieldPanel, BorderLayout.NORTH);

		//create instruction text fields
		//application convention is to map null to empty string (if db has null this will be shown as empty string)
		if(currentInstruction.getInstructionId() > 0) {
			instructionIdField = createLabelTextFieldPair(StringResources.getInstructionIdLabel(), ""+currentInstruction.getInstructionId(), lTextFieldPanel);
			instructionIdField.setEditable(false);
		}

		amountField = createLabelTextFieldPair(StringResources.getAmountLabel(),currentInstruction.getFrequency() == null ? "" : ""+ currentInstruction.getAmount(), lTextFieldPanel);
		frequencyField = createLabelTextFieldPair(StringResources.getFrequencyLabel(),currentInstruction.getFrequency() == null ? "" : ""+ currentInstruction.getFrequency(), lTextFieldPanel);
		if(currentInstruction.getInstructionId() > 0) {
			expiryDateField = createLabelTextFieldPair(StringResources.getExpiryDateLabel(), ""+currentInstruction.getExpiryDate(), lTextFieldPanel);
		}
		customerField = createLabelTextFieldPair(StringResources.getCustomerLabel(), currentInstruction.getCustomer() == null ? "" : ""+currentInstruction.getCustomer(), lTextFieldPanel);
		etfField = createLabelTextFieldPair(StringResources.getEtfLabel(), currentInstruction.getEtf() == null ? "" : ""+currentInstruction.getEtf(), lTextFieldPanel);
		add(lTextFieldPanel);

		//create notes text area
		notesArea = new JTextArea(currentInstruction.getNotes() == null ? "" : currentInstruction.getNotes());
		notesArea.setAutoscrolls(true);
		notesArea.setLineWrap(true);
		lPanelLayout.addLayoutComponent(notesArea, BorderLayout.CENTER);
		add(notesArea);
		
		//create instruction save (create or update button)
		JButton saveButton = createInstructionSaveButton();
		lPanelLayout.addLayoutComponent(saveButton, BorderLayout.SOUTH);
		this.add(saveButton);

		setLayout(lPanelLayout);
		updateUI();
	}

	private JButton createInstructionSaveButton() {
		JButton saveButton = new JButton(isUpdatePanel ? StringResources.getInstructionUpdateButtonLabel() : 
			StringResources.getInstructionAddButtonLabel());
		saveButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				//application convention is to map empty string to null (i.e. if app has empty string - this will be null in db)
				currentInstruction.setAmount(amountField.getText().equals("") ? null : amountField.getText());
				currentInstruction.setFrequency(frequencyField.getText().equals("") ? null : frequencyField.getText());
				if(isUpdatePanel) {
					currentInstruction.setExpiryDate(expiryDateField.getText().equals("") ? null : expiryDateField.getText());
				}
				currentInstruction.setCustomer(customerField.getText().equals("") ? null : customerField.getText());
				currentInstruction.setAdministrator(InstructionTrackerFrame.loggedInUsername);
				currentInstruction.setEtf(etfField.getText().equals("")  ? null : etfField.getText());
				currentInstruction.setNotes(notesArea.getText().equals("")  ? null : notesArea.getText());

				if(isUpdatePanel) {
					BusinessComponentFactory.getInstance().getInstructionProvider().updateInstruction(currentInstruction);
				}
				else {
					BusinessComponentFactory.getInstance().getInstructionProvider().addInstruction(currentInstruction);
				}
			}
		});
		
		return saveButton;
	}

	private JTextField createLabelTextFieldPair(String label, String value, JPanel container) {
		
		JPanel pairPanel = new JPanel();
		JLabel jlabel = new JLabel(label);
		JTextField field = new JTextField(value);
		pairPanel.add(jlabel);
		pairPanel.add(field);

		container.add(pairPanel);

		BorderLayout lPairLayout = new BorderLayout();
		lPairLayout.addLayoutComponent(jlabel, BorderLayout.WEST);
		lPairLayout.addLayoutComponent(field, BorderLayout.CENTER);
		pairPanel.setLayout(lPairLayout);	
		
		return field;
	}

	/**
	 * Implementation of IInstructionSelectionNotifiable::instructionSelected used to switch instruction
	 * displayed on InstructionDisplayPanel
	 */
	@Override
	public void instructionSelected(Instruction instruction) {
		initInstructionDetails(instruction);
	}
	
	/**
	 * Clear instruction details panel
	 */
	public void refresh()
	{
		initInstructionDetails(null);
	}
}

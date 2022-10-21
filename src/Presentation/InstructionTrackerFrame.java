package Presentation;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPasswordField;

import Business.BusinessComponentFactory;
import Business.Instruction;

public class InstructionTrackerFrame extends JFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5532618722097754725L;
	
	private AddEntitiesPanel addEntitiesPanel = null;
	private InstructionDetailPanel detailPanel = null;
	private InstructionSidePanel sidePanel = null;


	static String loggedInUsername = null;

	/**
	 * Main instruction tracker frame
	 * Logs on the user
	 * Initialize side panel + add entities panel + containing event list + detail panel
	 */
	public InstructionTrackerFrame()
	{
		setTitle(StringResources.getAppTitle());
	    setLocationRelativeTo(null);
	    
	    logOnUser();
	    initialise();
	    
	    setDefaultCloseOperation(EXIT_ON_CLOSE);  
	}
	
	/**
	 *  !!! 
	 *  Only used to simulate logon
	 *  This should really be implemented using proper salted hashing
	 *	and compare hash to that in DB
	 *  really should display an error message for bad login as well
	 *	!!!
	 */
	private void logOnUser() {
		boolean OK = false;
		while (!OK) {		
				String userName = (String)JOptionPane.showInputDialog(
									this,
									null,
									StringResources.getEnterUserNameString(),
									JOptionPane.QUESTION_MESSAGE);
				
				JPasswordField jpf = new JPasswordField();
				int okCancel = JOptionPane.showConfirmDialog(
									null,
									jpf,
									StringResources.getEnterPasswordString(),
									JOptionPane.OK_CANCEL_OPTION,
									JOptionPane.QUESTION_MESSAGE);
				
				String password = null;
				if (okCancel == JOptionPane.OK_OPTION) {
					password = new String(jpf.getPassword());
				}

				if (userName == null || password == null)
					System.exit(0);
				else
					if (!userName.isEmpty() && !password.isEmpty()) {
						loggedInUsername = checkAdmCredentials(userName, password);
						if (loggedInUsername != null) {
							OK = true;
						}
					}
		}
	}

	private void initialise()
	{
		addEntitiesPanel = new AddEntitiesPanel();	
	    detailPanel = new InstructionDetailPanel(true);	    
	    sidePanel = getSidePanel(new InstructionListPanel(getAllInstructions()));
	    
	    BorderLayout borderLayout = new BorderLayout();
	    borderLayout.addLayoutComponent(addEntitiesPanel, BorderLayout.NORTH);
	    borderLayout.addLayoutComponent(sidePanel, BorderLayout.WEST);
	    borderLayout.addLayoutComponent(detailPanel, BorderLayout.CENTER);
	    
	    JButton  refreshButton = new JButton(StringResources.getRefreshButtonLabel());
	    final InstructionTrackerFrame frame = this;
	    refreshButton.addActionListener(new ActionListener() {			
			@Override
			public void actionPerformed(ActionEvent e) {
				frame.refresh(frame.getAllInstructions());
			}
		});
	    
	    borderLayout.addLayoutComponent(refreshButton, BorderLayout.SOUTH);
	    
	    this.setLayout(borderLayout);
	    this.add(addEntitiesPanel);
	    this.add(refreshButton);
	    this.add(sidePanel);
	    this.add(detailPanel);
	    this.setSize(600, 300);
	}
	
	private InstructionSidePanel getSidePanel(InstructionListPanel listPanel)
	{
		final InstructionTrackerFrame frame = this;
		listPanel.registerEventSelectionNotifiableObject(detailPanel);
		return new InstructionSidePanel(new ISearchInstructionListener() {
			@Override
			public void searchClicked(String searchString) {
				frame.refresh(frame.findInstructionsByTitle(searchString));
			}
		},listPanel);
	}
	
	private String checkAdmCredentials(String userName, String password)
	{
		return BusinessComponentFactory.getInstance().getInstructionProvider().checkAdmCredentials(userName, password);
	}
	
	private Vector<Instruction> getAllInstructions()
	{
		return BusinessComponentFactory.getInstance().getInstructionProvider().findInstructionsByAdm(loggedInUsername);
	}
	
	private Vector<Instruction> findInstructionsByTitle(String pSearchString)
	{
		pSearchString = pSearchString.trim();
		if (!pSearchString.equals(""))
			return BusinessComponentFactory.getInstance().getInstructionProvider().findInstructionsByCriteria(pSearchString);
		else
			return BusinessComponentFactory.getInstance().getInstructionProvider().findInstructionsByAdm(loggedInUsername);
	}
	
	private  void refresh(Vector<Instruction> instructions)
	{
		if(sidePanel != null && detailPanel!= null)
		{
			sidePanel.refresh(instructions);
			detailPanel.refresh();
			sidePanel.registerEventSelectionNotifiableObject(detailPanel);
		}
	}
	
	
	public static void main(String[] args)
	{
        EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() {
            	InstructionTrackerFrame ex = new InstructionTrackerFrame();
                ex.setVisible(true);
            }
        });		
	}
}

package Business;

public class Instruction {

	private int instructionId = 0;
	private String amount;
	private String frequency;
	private String expiryDate;
	private String customer;
	private String administrator;
	private String etf;
	private String notes;


	public int getInstructionId() {
		return instructionId;
	}
	public void setInstructionId(int instructionId) {
		this.instructionId = instructionId;
	}
	
	public String getAmount() {
		return amount;
	}
	public void setAmount(String amount) {
		this.amount = amount;
	}

	public String getFrequency() {
		return frequency;
	}
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}

	public String getExpiryDate() {
		return expiryDate;
	}
	public void setExpiryDate(String expiryDate) {
		this.expiryDate = expiryDate;
	}

	public String getCustomer() {
		return customer;
	}
	public void setCustomer(String customer) {
		this.customer = customer;
	}

	public String getAdministrator() {
		return administrator;
	}
	public void setAdministrator(String administrator) {
		this.administrator = administrator;
	}

	public String getEtf() {
		return etf;
	}
	public void setEtf(String etf) {
		this.etf = etf;
	}

	public String getNotes() {
		return notes;
	}
	public void setNotes(String notes) {
		this.notes = notes;
	}
	
	public String toString()
	{
		return getCustomer() + " - " + getEtf();
	}
}

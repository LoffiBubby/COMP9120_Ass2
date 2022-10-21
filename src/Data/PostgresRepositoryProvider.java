package Data;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PSQLException;

import Business.Instruction;
import Presentation.IRepositoryProvider;

/**
 * Encapsulates create/read/update/delete operations to PostgreSQL database
 * @author IwanB
 */
public class PostgresRepositoryProvider implements IRepositoryProvider {
	//DB connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    private final String userid = "y22s2c9120_weli4073";
    private final String passwd = "Pyxcq6Ft";
    private final String myHost = "soit-db-pro-2.ucc.usyd.edu.au";

	private Connection openConnection() throws SQLException
	{
		PGSimpleDataSource source = new PGSimpleDataSource();
		source.setServerName(myHost);
		source.setDatabaseName(userid);
		source.setUser(userid);
		source.setPassword(passwd);
		Connection conn = source.getConnection();
	    
	    return conn;
	}

	/**
	 * Validate administrator login request
	 * @param userName: the user's userName trying to login
	 * @param password: the user's password used to login
	 * @return
	 */
	@Override
	public String checkAdmCredentials(String userName, String password) {
		try {
			Connection conn = openConnection();
			PreparedStatement ps = conn.prepareStatement("SELECT * FROM administrator WHERE login=? AND password=?");
			ps.setString(1,userName);
			ps.setString(2,password);
			ResultSet res = ps.executeQuery();
			conn.close();
			if(res.next()) {
//				System.out.println(res.getString("login"));
				return(res.getString("login"));
			}
		}
		catch (SQLException e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
			return null;
		}
		return null;
	}
	
	/**
	 * Find all associated instructions given an administrator
	 * @param userName: the administrator userName
	 * @return
	 */
	@Override
	public Vector<Instruction> findInstructionsByAdm(String userName) {
		try {
			Connection conn = openConnection();
			String sql = "\tSELECT investinstruction.instructionid, amount, frequency, expirydate, username, adminname, \"name\", notes, expirycode\n" +
					"\tFROM adminfullname\n" +
					"\tJOIN investinstruction ON adminfullname.\"login\" = investinstruction.administrator \n" +
					"\tJOIN customerfullname ON customerfullname.\"login\" = investinstruction.customer \n" +
					"\tJOIN etf ON investinstruction.code = etf.code\n" +
					"\tJOIN expirystatus ON investinstruction.instructionid = expirystatus.instructionid\n" +
					"\tWHERE investinstruction.administrator = ?\n" +
					"\tORDER BY expirystatus.expirycode DESC, expirydate ASC, username DESC;";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, userName);
			ResultSet res = ps.executeQuery();
			Vector<Instruction> ins = new Vector<Instruction>();
//			instruction.setInstructionId(res.getInt("instructionid"));
//			instruction.setAmount(res.getString("amount"));
//			instruction.setFrequency(res.getString("frequency"));
//			instruction.setExpiryDate(res.getString("expirydate"));
//			instruction.setCustomer(res.getString("username"));
//			instruction.setAdministrator(res.getString("adminname"));
//			instruction.setEtf(res.getString("etf.name"));
//			instruction.setNotes(res.getString("notes"));
//			ins.add(instruction);
			conn.close();
			while(res.next()){
				Instruction instruction = new Instruction();
				instruction.setInstructionId(res.getInt("instructionid"));
				instruction.setAmount(res.getString("amount"));
				instruction.setFrequency(res.getString("frequency"));
				instruction.setExpiryDate(res.getString("expirydate"));
				instruction.setCustomer(res.getString("username"));
				instruction.setAdministrator(res.getString("adminname"));
				instruction.setEtf(res.getString("name"));
				instruction.setNotes(res.getString("notes"));
				ins.add(instruction);
			}
			return ins;
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
//		Vector<Instruction> instructionVector = new Vector<Instruction>();
//		Instruction instruction = new Instruction();
//		instruction.setAmount("10000");
//		instruction.setInstructionId(1);
//		instruction.setFrequency("MTH");
//		instruction.setExpiryDate("2022-07-06");
//		instruction.setCustomer("Joyner Lucas");
//		instruction.setAdministrator("Weikun Li");
//		instruction.setEtf("111");
//		instruction.setNotes("aaaaa");
//
//		instructionVector.add(instruction);
//		return instructionVector;
//		return new Vector<Instruction>();
	}

	/**
	 * Find a list of instructions based on the searchString provided as parameter
	 * @param searchString: see assignment description for search specification
	 * @return
	 */
	@Override
	public Vector<Instruction> findInstructionsByCriteria(String searchString) {
		try {
			Connection conn = openConnection();
			String sql = "SELECT instructionid, amount, frequency, expirydate, username, adminname, \"name\", notes\n" +
					"FROM adminfullname\n" +
					"JOIN investinstruction ON adminfullname.\"login\" = investinstruction.administrator \n" +
					"JOIN customerfullname ON customerfullname.\"login\" = investinstruction.customer \n" +
					"JOIN etf ON investinstruction.code = etf.code\n" +
					"WHERE username LIKE ?\n" +
					"OR \"name\" LIKE ?\n" +
					"OR notes LIKE ?\n" +
					"AND expirydate >= '2022-10-21'::date\n" +
					"ORDER BY adminname, expirydate ASC;";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, "%"+searchString+"%");
			ps.setString(2, "%"+searchString+"%");
			ps.setString(3, "%"+searchString+"%");
			ResultSet res = ps.executeQuery();
			Vector<Instruction> ins = new Vector<Instruction>();
			conn.close();
			while(res.next()){
				Instruction instruction = new Instruction();
				instruction.setInstructionId(res.getInt("instructionid"));
				instruction.setAmount(res.getString("amount"));
				instruction.setFrequency(res.getString("frequency"));
				instruction.setExpiryDate(res.getString("expirydate"));
				instruction.setCustomer(res.getString("username"));
				instruction.setAdministrator(res.getString("adminname"));
				instruction.setEtf(res.getString("name"));
				instruction.setNotes(res.getString("notes"));
				ins.add(instruction);
				return ins;
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	/**
	 * Add a new instruction into the Database
	 * @param instruction: the new instruction to add
	 */
	@Override
	public void addInstruction(Instruction instruction) {

	}

	/**
	 * Update the details of a given instruction
	 * @param instruction: the instruction for which to update details
	 */
	@Override
	public void updateInstruction(Instruction instruction) {

	}
}

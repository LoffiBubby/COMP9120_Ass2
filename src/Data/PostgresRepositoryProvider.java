package Data;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Objects;
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
    private final String passwd = "Ljwwn001101";
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
			String sql = "SELECT\n" +
					"\tinvestinstruction.instructionid,\n" +
					"\tamount,\n" +
					"\tfrequencydesc,\n" +
					"\texpirydate,\n" +
					"\tusername,\n" +
					"\tadminname,\n" +
					"\t\"name\",\n" +
					"\tnotes,\n" +
					"\texpirycode \n" +
					"FROM\n" +
					"\tadminfullname\n" +
					"\tJOIN investinstruction ON adminfullname.\"login\" = investinstruction.administrator\n" +
					"\tJOIN customerfullname ON customerfullname.\"login\" = investinstruction.customer\n" +
					"\tJOIN etf ON investinstruction.code = etf.code\n" +
					"\tJOIN expirystatus ON investinstruction.instructionid = expirystatus.instructionid\n" +
					"\tJOIN frequency ON investinstruction.frequency = frequency.frequencycode \n" +
					"WHERE\n" +
					"\tinvestinstruction.administrator = ? \n" +
					"ORDER BY\n" +
					"\texpirystatus.expirycode DESC,\n" +
					"\texpirydate ASC,\n" +
					"\tusername DESC;";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, userName);
			ResultSet res = ps.executeQuery();
			Vector<Instruction> ins = new Vector<Instruction>();
			conn.close();
			while(res.next()){
				Instruction instruction = new Instruction();
				instruction.setInstructionId(res.getInt("instructionid"));
				instruction.setAmount(res.getString("amount"));
				instruction.setFrequency(res.getString("frequencydesc"));
				instruction.setExpiryDate(new SimpleDateFormat("dd-MM-yyyy").format(res.getDate("expirydate")));
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
			String sql = "SELECT\n" +
					"\tinvestinstruction.instructionid,\n" +
					"\tamount,\n" +
					"\tfrequencydesc,\n" +
					"\texpirydate,\n" +
					"\tusername,\n" +
					"\tadminname,\n" +
					"\t\"name\",\n" +
					"\tnotes,\n" +
					"\tadminname IS NULL AS is_admin_null\n" +
					"FROM\n" +
					"\tinvestinstruction\n" +
					"\tJOIN customerfullname ON customerfullname.\"login\" = investinstruction.customer\n" +
					"\tLEFT JOIN adminfullname ON adminfullname.\"login\" = investinstruction.administrator\n" +
					"\tJOIN etf ON investinstruction.code = etf.code\n" +
					"\tJOIN expirystatus ON investinstruction.instructionid = expirystatus.instructionid\n" +
					"\tJOIN frequency ON investinstruction.frequency = frequency.frequencycode \n" +
					"WHERE\n" +
					"\texpirycode = 1 \n" +
					"\tAND (username ILIKE ? \n" +
					"\tOR \"name\" ILIKE ? \n" +
					"\tOR notes ILIKE ? ) \n" +
					"ORDER BY\n" +
					"\tis_admin_null DESC,\n" +
					"\texpirydate ASC;";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, "%" + searchString + "%");
			ps.setString(2, "%" + searchString + "%");
			ps.setString(3, "%" + searchString + "%");
			ResultSet res = ps.executeQuery();
			Vector<Instruction> ins = new Vector<Instruction>();
			conn.close();
			while (res.next()) {
				Instruction instruction = new Instruction();
				instruction.setInstructionId(res.getInt("instructionid"));
				instruction.setAmount(res.getString("amount"));
				instruction.setFrequency(res.getString("frequencydesc"));
				instruction.setExpiryDate(new SimpleDateFormat("dd-MM-yyyy").format(res.getDate("expirydate"))); //??????
				instruction.setCustomer(res.getString("username"));
				instruction.setAdministrator(res.getString("adminname"));
				instruction.setEtf(res.getString("name"));
				instruction.setNotes(res.getString("notes"));
				ins.add(instruction);
			}
			return ins;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Add a new instruction into the Database
	 * @param instruction: the new instruction to add
	 */
	@Override
	public void addInstruction(Instruction instruction) {
		try{
			Connection conn = openConnection();
			String sql = "SELECT\n" +
					"\t* \n" +
					"FROM\n" +
					"\tinsert_into_investins ( ?, ?, ?, ?, ?);";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setBigDecimal(1, BigDecimal.valueOf(Double.parseDouble(instruction.getAmount())));
			String sql2 = "SELECT frequencycode FROM frequency WHERE frequencydesc = ?;"; // Use the Monthly or Fortnightly are acceptable
			PreparedStatement ps2 = conn.prepareStatement(sql2);
			ps2.setString(1,instruction.getFrequency());
			ResultSet rst1 = ps2.executeQuery();
			if(rst1.next())
				ps.setString(2, rst1.getString("frequencycode"));
			ps.setString(3, instruction.getCustomer());
			ps.setString(4, instruction.getEtf());
			ps.setString(5, instruction.getNotes());
			ps.executeQuery();
			conn.close();
			System.out.println("OK");
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Update the details of a given instruction
	 * @param instruction: the instruction for which to update details
	 */
	@Override
	public void updateInstruction(Instruction instruction) {
		try {
			Connection conn = openConnection();
			String sql = "SELECT\n" +
					"\t* \n" +
					"FROM\n" +
					"\tupdate_set_investins ( ?, ?, ?, ?, ?, ?, ?);";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setInt(1, instruction.getInstructionId());
			ps.setBigDecimal(2, BigDecimal.valueOf(Double.parseDouble(instruction.getAmount())));
			String sql2 = "SELECT frequencycode FROM frequency WHERE frequencydesc = ?;"; // Use the Monthly or Fortnightly are acceptable
			PreparedStatement ps2 = conn.prepareStatement(sql2);
			ps2.setString(1,instruction.getFrequency());
			ResultSet rst1 = ps2.executeQuery();
			if(rst1.next())
				ps.setString(3, rst1.getString("frequencycode"));
			ps.setDate(4, Date.valueOf(new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("dd-MM-yyyy").parse(instruction.getExpiryDate()))));
			ps.setString(5, instruction.getCustomer());
			ps.setString(6, instruction.getEtf());
			ps.setString(7, instruction.getNotes());
			ps.executeQuery();
			conn.close();
			System.out.println("OK");
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}

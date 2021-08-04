import java.lang.Math;
import java.util.Scanner;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.File;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;

/*This program creates an interactive prompt to enter the SQL commands 
  and also it redirects the flow to other functions based on the user's input
 */

public class Initial_Prompt_File{
	
	static String prompt = "SQL_CMD>";
	static String version = "v1.0a";
	static String copyright = "Lakshmi Priyanka @2020";
	static Boolean exit_flag = false;
	
	static String output_folder = "output";
	static String catalog_folder = "catalog";
	static String data_folder = "data";
	static String table_catalog = "davisbase_tables";
	static String column_catalog = "davisbase_columns";
	static String header_rowid = "row_id";
	static String header_table_name = "table_name";
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	
	static int table_catalog_bytesize = table_catalog.length();
	static int column_catalog_bytesize = column_catalog.length();
	
	static int payload_content_size = 2;
	static int row_id_size = 4;
	
	
	//static ArrayList<String> davisbase_tables = new ArrayList<String>();
	
	
	/*Initialize page size*/
	static int page_size = 512;
	
	/* Scanner class is used to read data from user
	   Delimiter of semicolon is used */
	static Scanner input = new Scanner(System.in).useDelimiter(";");
	
	/*Main method */
	public static void main(String[] args){
		
		/*Clear Screen for better presentation*/
		System.out.println("\n\n");
		System.out.println("*".repeat(80));
		System.out.println("Do you want to clear screen <Yes;/No;>?");
		String clrscr_cmd = "";
		clrscr_cmd = input.next().toLowerCase();
		if(clrscr_cmd.equals("yes"))
		{
			clrscr();
		}
		
		/*Display the welcome screen*/
		welcomeScreen();
		createDavisBaseCatalog();
		String usercommand ="";
		
		while(!exit_flag){
			System.out.print(prompt);
			/*remove Line feed character, Carriage return and trim the preceding 
			and trailing spaces. Convert to Lowercase
			*/
			usercommand = input.next().replace("\n"," ").replace("\r"," ").trim().toLowerCase();
			//System.out.println(usercommand);
			parseUserCommand(usercommand);
		}
		System.out.println("Exiting the prompt...");
		
	}
	
	public static void welcomeScreen(){
		System.out.println("*".repeat(80));
		System.out.println("Welcome to Lakshmi's DavisBase implementation");
		System.out.println("Version: "+version);
		System.out.println("Copyright: "+copyright);
		System.out.println("Type \"help;\" to display the supported commands");
		System.out.println("*".repeat(80));
		
	}
	
	public static void help_menu(){
		System.out.println("-".repeat(80));
		System.out.println("SUPPORTED COMMANDS\n");
		System.out.println("All commands below are case insensitive\n");
		System.out.println("SHOW TABLES;");
		System.out.println("\tDisplay the names of all tables.\n");
		System.out.println("SELECT \"column_list\" FROM table_name [WHERE condition];\n");
		System.out.println("\tDisplay table records whose optional condition");
		System.out.println("\tis <column_name> = <value>.\n");
		System.out.println("INSERT INTO (column1, column2, ...) table_name VALUES (value1, value2, ...);\n");
		System.out.println("\tInsert new record into the table.");
		//System.out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		//System.out.println("\tModify records data whose optional <condition> is\n");
		System.out.println("DROP TABLE table_name;");
		System.out.println("\tRemove table data (i.e. all records) and its schema.\n");
		System.out.println("VERSION;");
		System.out.println("\tDisplay the program version.\n");
		System.out.println("HELP;");
		System.out.println("\tDisplay this help information.\n");
		System.out.println("EXIT;");
		System.out.println("\tExit the program.\n");
		System.out.println("-".repeat(80));
	}
	
	public static void parseUserCommand(String usercommand){
	
		ArrayList<String> command_tokens = new ArrayList<String>(Arrays.asList(usercommand.split(" ")));
		//System.out.println(command_tokens);
		
		String first_token = command_tokens.get(0);
		
		//System.out.println(first_token);
		/* Clean up command string so that each token is separated by a single space */
		/*userCommand = userCommand.replaceAll("\n", " ");    // Remove newlines
		userCommand = userCommand.replaceAll("\r", " ");    // Remove carriage returns
		userCommand = userCommand.replaceAll(",", " , ");   // Tokenize commas
		userCommand = userCommand.replaceAll("\\(", " ( "); // Tokenize left parentheses
		userCommand = userCommand.replaceAll("\\)", " ) "); // Tokenize right parentheses
		userCommand = userCommand.replaceAll("( )+", " ");  // Reduce multiple spaces to a single space*/
			
		switch(first_token){
			case "help":
				System.out.println("Accessing Help Menu....");
				help_menu();
				break;
			case "exit":
				exit_flag=true;
				break;
			case "create":
				if(command_tokens.get(1).equals("table")){
					Commands.parseCreateTable(usercommand);
					//updateDavisBaseCatalog(usercommand);
				}
				break;
			case "show":
				if(command_tokens.get(1).equals("tables")){
					showDavisbaseTables();
				}
				if(command_tokens.get(1).equals("columns")){
					showDavisbaseColumns();
				}
				break;
			case "drop":
				if(command_tokens.get(1).equals("table")){
					Commands.parseDropTable(usercommand);
				}
				break;
			case "insert":
				Commands.parseInsert(usercommand);
				break;
			case "delete":
				//parseDeleteQuery(usercommand);
			case "select":
				
				if(usercommand.trim().equals("select * from "+column_catalog))
					{
					
						showDavisbaseColumns();
					}
				if(usercommand.trim().equals("select * from "+table_catalog))
					{
					
						showDavisbaseTables();
					}
				else{
					Commands.parseSelect(usercommand);	
				}
				break;
				
				
				//parseSelectQuery(usercommand);
			case "update":
				//parseUpdateQuery(usercommand);
			default:
				System.out.println("The command you have entered doesn't match the items listed in the menu!");
				System.out.println("#".repeat(20)+" Please verify & retype "+"#".repeat(20));
			
		}
	}
	
	public static void createDavisBaseCatalog(){
		
		//ArrayList<String> command_tokens = new ArrayList<String>(Arrays.asList(usercommand.split(" ")));
		
		//String table_name = command_tokens.get(2);
		//System.out.println("Tablename: " + table_name);
		//davisbase_tables.add(table_name);
		//System.out.println(davisbase_tables);
		
		//check if the directory exists, if it exists, just write the name to the file
		// else create the directory and create a new file and write into the file
		String working_dir = System.getProperty("user.dir");
		String file_storage_path = working_dir + '\\' + output_folder;
		/*working_dir gets the current directory path and checks if 
		the output directory is already available
		*/
		/*if the output directory is not available, an output directory is created
		*/
		if(!checkDirectory(working_dir, output_folder)){
			if(!createDirectory(working_dir, output_folder)){
				System.out.println("Problem occured in creating folder:" + output_folder);
			}
		}
		if(!checkDirectory(file_storage_path, catalog_folder)){
			if(!createDirectory(file_storage_path, catalog_folder)){
				System.out.println("Problem occured in creating folder:" + catalog_folder);	
			}
		}
		if(!checkDirectory(file_storage_path, data_folder)){
			if(!createDirectory(file_storage_path, data_folder)){
				System.out.println("Problem occured in creating folder:" + data_folder);
			}
		}
		//Check if the davisbase_tables & davisbase_columns files exists
		
		String catalog_path = file_storage_path +'\\'+catalog_folder;
		
		//table_catalog, column_catalog
		String table_catalog_file = table_catalog + ".tbl";
		String column_catalog_file = column_catalog + ".tbl";
		
		//Checks if the catalog files exists, if not creates the files
	if(!checkFileExists(catalog_path,table_catalog_file)){
		try {
			String file_path = catalog_path + '\\'+table_catalog_file;
			RandomAccessFile davisbase_tbl_file = new RandomAccessFile(file_path,"rw");
			davisbase_tbl_file.setLength(page_size);
			
			davisbase_tbl_file.seek(0);
			davisbase_tbl_file.writeByte(0x0D); //One byte flag to identify page type
			davisbase_tbl_file.seek(2);
			davisbase_tbl_file.writeShort(0x02); // Two byte flag to denote the number of cells in the page
			//We have two catalog tables, hence we enter the value 2
			
			//Calculate the offset for record 1 from the end-of-the-file
			//(Hardcoded 2: Byte 1: No_of_columns(1) and Byte 2: type_of_column))
			int davisbase_table_offset = page_size-(30); 
			int davisbase_column_offset = davisbase_table_offset-(31);
			
			//System.out.println(davisbase_table_offset);
			//System.out.println(davisbase_column_offset);
			davisbase_tbl_file.writeShort(davisbase_column_offset);
			davisbase_tbl_file.seek(0x06);
			davisbase_tbl_file.writeInt(0xffffffff); //No right sibling
			davisbase_tbl_file.seek(0x0A);
			davisbase_tbl_file.writeInt(0xffffffff); //No PARENT
			davisbase_tbl_file.seek(0x10);
			davisbase_tbl_file.writeShort(davisbase_table_offset);
			davisbase_tbl_file.writeShort(davisbase_column_offset);
			
			davisbase_tbl_file.seek(davisbase_table_offset);
			davisbase_tbl_file.writeShort(24);
			davisbase_tbl_file.writeInt(0x01); //row_id
			davisbase_tbl_file.writeByte(0x04); //contains four columns
			davisbase_tbl_file.writeByte(table_catalog_bytesize+12);
			davisbase_tbl_file.writeByte(1); //root page is 
			davisbase_tbl_file.writeByte(1); //record count size
			davisbase_tbl_file.writeByte(1); //avg_record size
			davisbase_tbl_file.writeBytes(table_catalog);
			davisbase_tbl_file.writeByte(0x00);
			davisbase_tbl_file.writeByte(0x02);
			davisbase_tbl_file.writeByte(0x20);
			
			davisbase_tbl_file.seek(davisbase_column_offset);
			davisbase_tbl_file.writeShort(25);
			davisbase_tbl_file.writeInt(0x02); //row_id
			davisbase_tbl_file.writeByte(0x04); //contains four column
			davisbase_tbl_file.writeByte(column_catalog_bytesize+12);
			davisbase_tbl_file.writeByte(1);
			davisbase_tbl_file.writeByte(1);
			davisbase_tbl_file.writeByte(1);
			davisbase_tbl_file.writeBytes(column_catalog);
			davisbase_tbl_file.writeByte(0x00);
			davisbase_tbl_file.writeByte(12);
			davisbase_tbl_file.writeByte(0x20);
		}
		catch(Exception e){
			System.out.println(e);	
		}
	}
	
	if(!checkFileExists(catalog_path,column_catalog_file)){
		try{
			String file_path = catalog_path + '\\'+column_catalog_file;
			RandomAccessFile davisbase_clm_file = new RandomAccessFile(file_path,"rw");
			davisbase_clm_file.setLength(page_size);
			davisbase_clm_file.seek(0);
			davisbase_clm_file.writeByte(0x0D);
			davisbase_clm_file.seek(2);
			davisbase_clm_file.writeShort(0x08); /*We have a total of 12 initial values*/
			
			int[] offset = new int[12];
			offset[0] = page_size - 43; //row_id
			offset[1] = offset[0] - 48;  //table_name
			offset[2] = offset[1] - 51; //root_page
			offset[3] = offset[2] - 49; //record_count
			offset[4] = offset[3] - 47; //avg_record
			offset[5] = offset[4] - 44; 
			offset[6] = offset[5] - 49;
			offset[7] = offset[6] - 50; //write
			
			offset[8] = (2*page_size) - 48; //This overflows to the next page
			offset[9] = offset[8] - 58;
			offset[10] = offset[9] - 50;
			offset[11] = offset[10] - 48;
			
			
			davisbase_clm_file.writeShort(offset[7]);
			davisbase_clm_file.writeInt(0x02); // Sibling page to the right
			davisbase_clm_file.seek(0x0A);
			davisbase_clm_file.writeInt(0xFFFFFFFF); //Parent page
			davisbase_clm_file.skipBytes(2);
			
			for(int j=0; j<8;j++){			
			davisbase_clm_file.writeShort(offset[j]); 	
			}
			
			
			davisbase_clm_file.seek(offset[0]);
			davisbase_clm_file.writeShort(37);
			davisbase_clm_file.writeInt(1);
			davisbase_clm_file.writeByte(6);
			davisbase_clm_file.writeByte(28);
			davisbase_clm_file.writeByte(18);
			davisbase_clm_file.writeByte(15);
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(table_catalog);
			davisbase_clm_file.writeBytes(header_rowid);
			davisbase_clm_file.writeBytes("INT");
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			davisbase_clm_file.seek(offset[1]);
			davisbase_clm_file.writeShort(42);
			davisbase_clm_file.writeInt(2);
			davisbase_clm_file.writeByte(6); //no_of_columns
			davisbase_clm_file.writeByte(28); //davisbase_tables+12
			davisbase_clm_file.writeByte(22); // table_name+12
			davisbase_clm_file.writeByte(16); // Text+12
			davisbase_clm_file.writeByte(1); 
			davisbase_clm_file.writeByte(14); // 2+12
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(table_catalog);
			davisbase_clm_file.writeBytes(header_table_name);
			davisbase_clm_file.writeBytes("TEXT");
			davisbase_clm_file.writeByte(2);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			davisbase_clm_file.seek(offset[2]);
			davisbase_clm_file.writeShort(45);
			davisbase_clm_file.writeInt(3);
			davisbase_clm_file.writeByte(6); //no_of_columns
			davisbase_clm_file.writeByte(28); //davisbase_tables+12
			davisbase_clm_file.writeByte(21); // root_page+12
			davisbase_clm_file.writeByte(20); // smallint+12
			davisbase_clm_file.writeByte(1); //
			davisbase_clm_file.writeByte(14); // 2+12
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(table_catalog);
			davisbase_clm_file.writeBytes("root_page");
			davisbase_clm_file.writeBytes("SMALLINT");
			davisbase_clm_file.writeByte(3);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			davisbase_clm_file.seek(offset[3]);
			davisbase_clm_file.writeShort(43);
			davisbase_clm_file.writeInt(4);
			davisbase_clm_file.writeByte(6); //no_of_columns
			davisbase_clm_file.writeByte(28); //davisbase_tables+12
			davisbase_clm_file.writeByte(24); // record_count+12
			davisbase_clm_file.writeByte(15); // int+12
			davisbase_clm_file.writeByte(1); 
			davisbase_clm_file.writeByte(14); // 2+12
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(table_catalog);
			davisbase_clm_file.writeBytes("record_count");
			davisbase_clm_file.writeBytes("INT");
			davisbase_clm_file.writeByte(4);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			davisbase_clm_file.seek(offset[4]);
			davisbase_clm_file.writeShort(41);
			davisbase_clm_file.writeInt(5);
			davisbase_clm_file.writeByte(6); // no_of_columns
			davisbase_clm_file.writeByte(28); //davisbase_tables+12
			davisbase_clm_file.writeByte(22); // avg_record+12
			davisbase_clm_file.writeByte(15); // int+12
			davisbase_clm_file.writeByte(1); 
			davisbase_clm_file.writeByte(14); // 2+12
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(table_catalog);
			davisbase_clm_file.writeBytes("avg_record");
			davisbase_clm_file.writeBytes("INT");
			davisbase_clm_file.writeByte(5);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			davisbase_clm_file.seek(offset[5]);
			davisbase_clm_file.writeShort(38);
			davisbase_clm_file.writeInt(6);
			davisbase_clm_file.writeByte(6);
			davisbase_clm_file.writeByte(29); 
			davisbase_clm_file.writeByte(18);// row_id +12
			davisbase_clm_file.writeByte(15); 
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(column_catalog);
			davisbase_clm_file.writeBytes(header_rowid);
			davisbase_clm_file.writeBytes("INT");
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			
			davisbase_clm_file.seek(offset[6]);
			davisbase_clm_file.writeShort(44);
			davisbase_clm_file.writeInt(7);
			davisbase_clm_file.writeByte(6);
			davisbase_clm_file.writeByte(29);
			davisbase_clm_file.writeByte(22);
			davisbase_clm_file.writeByte(16);
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(column_catalog);
			davisbase_clm_file.writeBytes(header_table_name);
			davisbase_clm_file.writeBytes("TEXT");
			davisbase_clm_file.writeByte(2);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			davisbase_clm_file.seek(offset[7]);
			davisbase_clm_file.writeShort(45);
			davisbase_clm_file.writeInt(8);
			davisbase_clm_file.writeByte(6);
			davisbase_clm_file.writeByte(29);
			davisbase_clm_file.writeByte(23);
			davisbase_clm_file.writeByte(16);
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(column_catalog);
			davisbase_clm_file.writeBytes("Column_name");
			davisbase_clm_file.writeBytes("TEXT");
			davisbase_clm_file.writeByte(3);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			davisbase_clm_file.setLength(page_size*2);
			davisbase_clm_file.seek(page_size);
			davisbase_clm_file.writeByte(0x0D);
			davisbase_clm_file.skipBytes(1);
			davisbase_clm_file.writeShort(0x04);
			
			davisbase_clm_file.writeShort(offset[11]);
			davisbase_clm_file.writeInt(0xFFFFFFFF); // Sibling page to the right
			davisbase_clm_file.writeInt(0xFFFFFFFF); //parent page
			davisbase_clm_file.skipBytes(2);
			
			for(int j=8; j<12;j++){
			davisbase_clm_file.writeShort(offset[j]); 	
			}
			
			
			davisbase_clm_file.seek(offset[8]);
			davisbase_clm_file.writeShort(43);
			davisbase_clm_file.writeInt(9);
			davisbase_clm_file.writeByte(6);
			davisbase_clm_file.writeByte(29);
			davisbase_clm_file.writeByte(21);
			davisbase_clm_file.writeByte(16);
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(column_catalog);
			davisbase_clm_file.writeBytes("Data_type");
			davisbase_clm_file.writeBytes("TEXT");
			davisbase_clm_file.writeByte(4);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			davisbase_clm_file.seek(offset[9]);
			davisbase_clm_file.writeShort(53);
			davisbase_clm_file.writeInt(10);
			davisbase_clm_file.writeByte(6);
			davisbase_clm_file.writeByte(29);
			davisbase_clm_file.writeByte(28);
			davisbase_clm_file.writeByte(19);
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(column_catalog);
			davisbase_clm_file.writeBytes("Ordinal_position");
			davisbase_clm_file.writeBytes("TINYINT");
			davisbase_clm_file.writeByte(5);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			davisbase_clm_file.seek(offset[10]);
			davisbase_clm_file.writeShort(45);
			davisbase_clm_file.writeInt(11);
			davisbase_clm_file.writeByte(6);
			davisbase_clm_file.writeByte(29);
			davisbase_clm_file.writeByte(23);
			davisbase_clm_file.writeByte(16);
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(column_catalog);
			davisbase_clm_file.writeBytes("IS_NULLABLE");
			davisbase_clm_file.writeBytes("TEXT");
			davisbase_clm_file.writeByte(6);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
			
			
		
			davisbase_clm_file.seek(offset[11]);
			davisbase_clm_file.writeShort(43);
			davisbase_clm_file.writeInt(12);
			davisbase_clm_file.writeByte(6);
			davisbase_clm_file.writeByte(29);
			davisbase_clm_file.writeByte(21);
			davisbase_clm_file.writeByte(16);
			davisbase_clm_file.writeByte(1);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeByte(14);
			davisbase_clm_file.writeBytes(column_catalog);
			davisbase_clm_file.writeBytes("IS_UNIQUE");
			davisbase_clm_file.writeBytes("TEXT");
			davisbase_clm_file.writeByte(7);
			davisbase_clm_file.writeBytes("NO");
			davisbase_clm_file.writeBytes("NO");
				
		}
	
		catch(Exception e){
			System.out.println(e);
		}
	} 
	}
	
	public static boolean checkFileExists(String file_path, String file_name){
		
		File file_pointer = new File(file_path + '\\'+file_name);
		boolean exists_flag = file_pointer.exists();
		return exists_flag;
		
	}		
	
	public static void showDavisbaseTables(){
	//code to display all tables Davisbasetables
		String working_dir = System.getProperty("user.dir");
		String catalog_path = working_dir + '\\' + output_folder + '\\' + catalog_folder;
		String filename = table_catalog + ".tbl";
		
		try
		{
			RandomAccessFile davisbase_tbl_file = new RandomAccessFile(catalog_path +"\\"+ filename , "rw");
			int no_of_pages = Commands.getPageCount(davisbase_tbl_file);
			
			/*for every page repeat this process*/
			for( int k = 0; k < no_of_pages; k++){
			
				davisbase_tbl_file.seek(k*page_size);
				davisbase_tbl_file.skipBytes(2);
				int no_of_records = (int) davisbase_tbl_file.readShort();
				davisbase_tbl_file.skipBytes(12);
				int[] record_offset = new int[no_of_records];
				
				for(int s = 0; s < no_of_records; s++){
				
					record_offset[s] = davisbase_tbl_file.readShort();
					
				}
				/*Print the header for Davisbase tables */
				System.out.println("-".repeat(120));
				System.out.print(String.format("%-20s","row_id")+"|");
				System.out.print(String.format("%-20s","Table_name")+"|");
				System.out.print(String.format("%-20s","root_page")+"|");
				System.out.print(String.format("%-20s","record_count")+"|");
				System.out.println(String.format("%-20s","avg_record")+"|");
				System.out.println("-".repeat(120));
				
				
				for(int r = 0; r < no_of_records; r++){
					
					String[] values = null;
					values = retrieveValues(davisbase_tbl_file, record_offset[r]);
					
					for (int w=0; w < values.length;w++)
					{
						//System.out.println(values[w]);
					System.out.print(String.format("%-20s",values[w])+"|");
					}
					System.out.println();	
				}
						
			}
					
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
				
		
				
	}
	
	public static void showDavisbaseColumns(){
	//code to display all tables Davisbasetables
		String working_dir = System.getProperty("user.dir");
		String catalog_path = working_dir + '\\' + output_folder + '\\' + catalog_folder;
		String filename = column_catalog + ".tbl";
		
		System.out.println("-".repeat(150));
		System.out.print(String.format("%-20s","row_id")+"|");
		System.out.print(String.format("%-20s","Table_name")+"|");
		System.out.print(String.format("%-20s","column_name")+"|");
		System.out.print(String.format("%-20s","data_type")+"|");
		System.out.print(String.format("%-20s","Ordinal_position")+"|");
		System.out.print(String.format("%-20s","IS_NULLABLE")+"|");
		System.out.println(String.format("%-20s","IS_UNIQUE")+"|");
		System.out.println("-".repeat(150));
		
		try
		{
			RandomAccessFile davisbase_clm_file = new RandomAccessFile(catalog_path +"\\"+ filename , "rw");
			int no_of_pages = Commands.getPageCount(davisbase_clm_file);
			
			for( int k = 0; k < no_of_pages; k++){
			
				davisbase_clm_file.seek( k*page_size);
				davisbase_clm_file.skipBytes(2);
				int no_of_records = (int) davisbase_clm_file.readShort();
				davisbase_clm_file.skipBytes(12);
				int[] record_offset = new int[no_of_records];
				
				for(int s = 0; s < no_of_records; s++){
				
					record_offset[s] = davisbase_clm_file.readShort();
					
				}
				/*Print the header for Davisbase tables */
				
				
				for(int r = 0; r < no_of_records; r++){
					
					
					String[] values = null;
					values = retrieveValues(davisbase_clm_file, record_offset[r]);
					
					for (int w=0; w < values.length;w++)
					{
						//System.out.println(values[w]);
					System.out.print(String.format("%-20s",values[w])+"|");
					}
					System.out.println();	
				}
			
		}
		}
		catch(Exception e){
			System.out.println(e);	
		}
	
		
	}
	
	public static void getAllColumns(){
	//code to display all columns DavisBase columns
	}
	
	public static boolean checkDirectory(String dir_path, String folder_name){
		
		/*
		This method is used to check whether the directory for catalog and data
		already exists. 
		*/
		Boolean dir_flag= false;
	    File dir_pointer = new File(dir_path);
        File[] files = dir_pointer.listFiles();
        FileFilter fileFilter = new FileFilter() {
         public boolean accept(File file) {
            return file.isDirectory();
         }
      }; 
      for (int i = 0; i< files.length; i++) {
            File filename = files[i];
            if(filename.isDirectory()){
            	if (filename.getName().equals(folder_name)){
            		dir_flag= true;
            	}
            }	
      }
      return dir_flag;
   }
   
   public static boolean createDirectory(String dir_path, String folder_name){
   	   
   	   /*This is where I left off */
   	   /*This method creates a new directory with the given folder_name in
   	   the given path. This provides for creating the output, catalog and the 
   	   data folders
   	   */
   	   Boolean create_dir_status = false;
   	   File name = new File(dir_path+'/'+folder_name);
   	   
   	   create_dir_status = name.mkdir();
   	   return create_dir_status;
   }
   
   public static void clrscr(){
   	   try{
   	   	   new ProcessBuilder("cmd","/c","cls").inheritIO().start().waitFor();
   	   }
   	   catch(Exception e){
   	   	   System.out.println(e);
   	   }
   }

	
	public static String[] retrieveValues(RandomAccessFile file, long loc){
	
		String[] values = null;
		
		try{
			
			SimpleDateFormat dateFormat = new SimpleDateFormat (datePattern);
			
			file.seek(loc+2);
			//System.out.println(file.getFilePointer());
			int row_id= file.readInt();
			int num_of_cols = file.readByte();
			
			byte[] typeCode = new byte[num_of_cols];
			
			//System.out.println("Retrieve values row_id: "+row_id +" " +num_of_cols);
			file.read(typeCode);
			
			values = new String[num_of_cols+1];
			
			values[0] = Integer.toString(row_id);
			
			
			for(int i=1; i <= num_of_cols; i++){
				switch(typeCode[i-1]){
					case 0:  file.readByte();
					    values[i] = "null";
						break;
					case 1:  
					    values[i] = Integer.toString(file.readByte());
						break;

					case 2:  
					    values[i] = Integer.toString(file.readShort());
						break;

					case 3:  
					    values[i] = Integer.toString(file.readInt());
						break;

					case 4:  
						values[i] = Long.toString(file.readLong());
						break;

					case 5:  
						values[i] = Float.toString(file.readFloat());
						break;

					case 6:  
						values[i] = Double.toString(file.readDouble());
						break;

					case 8:  
						values[i] = Long.toString(file.readByte());
						break;
						
					case 9:  
						int temp = file.readInt();
						Date dateTime = new Date(temp);
						values[i] = dateFormat.format(dateTime);
						break;

					case 10:  
						Long temp2 = file.readLong();
						Date datetime = new Date(temp2);
						values[i] = dateFormat.format(datetime);
						break;

					case 11:  
						Long temp3 = file.readLong();
						Date date = new Date(temp3);
						values[i] = dateFormat.format(date).substring(0,10);
						break;

					//text case
					default:    int len = typeCode[i-1]-0x0C;
								byte[] bytes = new byte[len];
								file.read(bytes);
								values[i] = new String(bytes);
								break;
			}
			
			}
		}
		catch(Exception e)
		{
			System.out.println(e);
		}

		return values;
	}		
	
	

}
		
	   
	
	
	
	
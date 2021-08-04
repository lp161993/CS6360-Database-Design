import static java.lang.System.out;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class Commands {
	
	/* This method determines what type of command the userCommand is and
	 * calls the appropriate method to parse the userCommand String. 
	 */
	 
	static int page_size = Initial_Prompt_File.page_size;
	static int payload_content_size = Initial_Prompt_File.payload_content_size;
	static int row_id_size = Initial_Prompt_File.row_id_size;
	static String output_folder = "output";
	static String catalog_folder = "catalog";
	static String data_folder = "data";
	static String table_catalog = "davisbase_tables";
	static String column_catalog = "davisbase_columns";
	
	public static void parseUserCommand (String userCommand) {
		
	}

	public static void parseCreateTable(String command) {
		/* TODO: Before attempting to create new table file, check if the table already exists */
		
		
		System.out.println("Stub: parseCreateTable method");
		System.out.println("Command: " + command);
		ArrayList<String> commandTokens = commandStringToTokenList(command);

		/* Extract the table name from the command string token list */
		String tableFileName = commandTokens.get(2) + ".tbl";
		String working_dir = System.getProperty("user.dir");
		String data_path = working_dir + '\\' + Initial_Prompt_File.output_folder + '\\' + Initial_Prompt_File.data_folder;
		
		//Method to check if the file already exists
		if(!Initial_Prompt_File.checkFileExists(data_path, tableFileName)){
			try {
				/*  Create RandomAccessFile tableFile in read-write mode.
				 *  Note that this doesn't create the table file in the correct directory structure
				 */
				/* Create a new table file whose initial size is one page (i.e. page size number of bytes) */
				RandomAccessFile tableFile = new RandomAccessFile(data_path +"\\"+  tableFileName, "rw");
				tableFile.setLength(Initial_Prompt_File.page_size);
	
				/* Write page header with initial configuration */
				tableFile.seek(0);
				tableFile.writeByte(0x0D);       // Page type
				tableFile.seek(0x02);
				tableFile.writeShort(0x00);  
				tableFile.writeShort(0x01FF);   // Offset beginning of cell content area				
				tableFile.writeInt(0xFFFFFFFF); // Sibling page to the right
				tableFile.writeInt(0xFFFFFFFF); // Parent page 
				tableFile.close();
			}
			catch(Exception e) {
				System.out.println(e);
			}
			
		/*  Code to insert an entry in the TABLES meta-data for this new table.
		 *  i.e. New row in davisbase_tables if you're using that mechanism for meta-data.
		 */
		
		/*  Code to insert entries in the COLUMNS meta data for each column in the new table.
		 *  i.e. New rows in davisbase_columns if you're using that mechanism for meta-data.
		 */
			UpdateDavisbaseTables(tableFileName);
			
			String cols = command.split(commandTokens.get(2))[1].trim();
			String[] create_cols = cols.substring(1, cols.length()-1).split(",");
		
		
			int col_no =1; 
			
		for(String i: create_cols){
			
			String col_name = "";
			String col_type = "";
			boolean nullable  = true;
			boolean unique = false;
			
		
			if( i.split(" ").length > 1){	
				i = i.trim();
				//System.out.println("i: "+i);
				//System.out.println("i: "+i.split(" ").length);
				//System.out.println("i: "+i.split(" ")[0]);
				col_name = i.split(" ")[0];
				col_type = i.split(" ")[1];
				if(i.contains("primary")){
					unique = true;
					nullable = false;
				}
				if(i.contains("not null")){
					nullable = false;
				}
				//System.out.println("1:"+col_name+"2:"+col_type+"3:"+col_no+"4:"+nullable+"5:"+unique);
				updateDavisbaseColumns(commandTokens.get(2), col_name, col_type, col_no++, nullable, unique); 
				//System.out.println(i.trim());
			}
		}
		
		}
		else{
			System.out.println("A table already exists in this name, please provide another name!");
		}
	}

	/*
	 *  Stub method for inserting a new record into a table.
	 */
	public static void parseInsert(String command) {
		System.out.println("Command: " + command);
		System.out.println("Stub: This is the insert Record method");
		/* TODO: Your code goes here */
		ArrayList<String> commandTokens = commandStringToTokenList(command);
		
		String tableFileName = commandTokens.get(2) + ".tbl";
		String working_dir = System.getProperty("user.dir");
		String data_path = working_dir + '\\' + output_folder + '\\' + data_folder;
		
		String table_name = commandTokens.get(2);
		String command_iter = command.replace(("insert into " + table_name),"");
		String[] col_split = command_iter.split("values");
		
		String[] col_names = col_split[0].replace("(","").replace(")","").split(",");
		String[] col_vals = col_split[1].replace("(","").replace(")","").split(",");
		
		int offset = payload_content_size + row_id_size + (col_names.length+1);
		int size = 0;
		int[][] all_col_details = new int[col_names.length][2];
		
		/*find column type in davisbase col*/
		for(int i =0; i< col_names.length; i++){
			
			int[] col_detail = getColDetails(table_name, col_names[i]); //returns size & code
		    
			
			String content = col_vals[i].replaceAll("\'","").trim();
			col_vals[i] = content;
			if(col_detail[1] == 12){
				col_detail[0] = content.length();
				col_detail[1] = content.length()+12; //code for text
			}
			all_col_details[i] = col_detail;
			offset +=col_detail[0];
		}
		
		
		int payload = offset - (payload_content_size + row_id_size);
		boolean new_page = false;
		int prev_row_no =0;
		int row_no;

		if(Initial_Prompt_File.checkFileExists(data_path, tableFileName)){
			try {
				/*  Create RandomAccessFile tableFile in read-write mode.
				 */
				/* Create a new table file whose initial size is one page (i.e. page size number of bytes) */
				RandomAccessFile tablefile = new RandomAccessFile(data_path +"\\"+  tableFileName, "rw");
				int no_of_pages = getPageCount(tablefile);
				
				if(!checkByteAvailability(tablefile)){
			 	 
					System.out.println("Not enough space on current page");
					new_page = true;
					tablefile.setLength((no_of_pages+1)*page_size);
					tablefile.seek((no_of_pages-1)* page_size);
					tablefile.skipBytes(4);
					int prev_content = (int) tablefile.readShort();
					tablefile.seek(prev_content);
					tablefile.skipBytes(2); //skip payload
					prev_row_no = tablefile.readInt();
					tablefile.seek((no_of_pages-1)* page_size);
					tablefile.skipBytes(6);
					tablefile.writeInt(no_of_pages+1); // right Sibling
					if (no_of_pages!=1){tablefile.writeInt(no_of_pages-1);}
					
	
					tablefile.seek(no_of_pages* page_size);
					tablefile.writeByte(0x0D); //One byte flag to identify page type
					tablefile.skipBytes(2);
					tablefile.writeShort(0x00);
					tablefile.writeShort(no_of_pages* page_size);
					tablefile.writeInt(0xffffffff); //No right Sibling
					tablefile.writeInt(0xffffffff); //NO parent
			 	}
			 	
			 	no_of_pages = getPageCount(tablefile);
			 	
			    tablefile.seek((no_of_pages-1)* page_size);
			    tablefile.skipBytes(2);
			    int fp = (int)tablefile.getFilePointer();
			    int no_of_cells = (int) tablefile.readShort(); //retrieve row nos
			    tablefile.seek(fp);
			    tablefile.writeShort(no_of_cells+1); //update row nos
					
			    if(no_of_cells ==0){
			    	int initial_offset = (no_of_pages*page_size)-offset;
			    	tablefile.writeShort(initial_offset);
			    	tablefile.skipBytes(10);
			    	tablefile.writeShort(initial_offset);
			    	tablefile.seek(initial_offset);
			    	row_no = 0;
			    }
			    else{
					fp = (int)tablefile.getFilePointer();
					int content_begins = (int) tablefile.readShort();
					tablefile.seek(content_begins);
					tablefile.skipBytes(2); //skip payload
					row_no = tablefile.readInt(); // last row no
					int new_content_begins = content_begins - offset;
					tablefile.seek(fp);
					tablefile.writeShort(new_content_begins);
					tablefile.skipBytes(10);
					tablefile.skipBytes(no_of_cells*2);
					tablefile.writeShort(new_content_begins);
					tablefile.seek(new_content_begins);
			    }
			    tablefile.writeShort(payload);
			    if (new_page){ row_no = prev_row_no; }
			    tablefile.writeInt(row_no+1);
			    tablefile.writeByte(col_names.length);
			    
			    for(int i =0; i < col_names.length; i++){
			    
			    	tablefile.writeByte(all_col_details[i][1]);
			    }
			    
			    for(int i=0; i< col_names.length; i++){
			    	
			    	int code = all_col_details[i][1];
			    	switch(code){
			    		case 1:    
			    			tablefile.writeByte(Integer.parseInt(col_vals[i]));
			    			break;
			    		case 2:   
			    			tablefile.writeShort(Integer.parseInt(col_vals[i]));
			    			break;
			    		case 3:	  
			    			tablefile.writeInt(Integer.parseInt(col_vals[i]));
			    			break;
			    		case 4:
			    			tablefile.writeLong(Long.parseLong(col_vals[i]));
			    			break;
			    		case 5:
			    			tablefile.writeFloat(Float.parseFloat(col_vals[i]));
			    			break;
			    		case 6:
			    			tablefile.writeDouble(Double.parseDouble(col_vals[i]));
			    			break;
			    		case 8:
			    			tablefile.writeByte(Integer.parseInt(col_vals[i]));
			    			break;
			    		case 10:
			    			tablefile.writeBytes(col_vals[i]);
			    			break;
			    		case 11:
			    			tablefile.writeBytes(col_vals[i]);
			    			break;
			    		case 12:
			    			tablefile.writeBytes(col_vals[i]);
			    			break;
			    		default:
			    			tablefile.writeBytes(col_vals[i]);
			    			break;
			    	}
			    	
			    }
			tablefile.close();
			}
			    	 	
			catch(Exception e)
			{
				System.out.println(e);
			}
			
		}
	}
	public static int[] getColDetails(String table_name, String col_name){
	
		String working_dir = System.getProperty("user.dir");
		String catalog_path = working_dir + '\\' + output_folder + '\\' + catalog_folder;
		String filename = column_catalog + ".tbl";
		String datatype = "";
		int size = 0;
		
		try
		{
			RandomAccessFile davisbase_clm_file = new RandomAccessFile(catalog_path +"\\"+ filename , "rw");
			int no_of_pages = Commands.getPageCount(davisbase_clm_file);
			
			
			
			for( int k = 0; k < no_of_pages; k++){
			
				 
				davisbase_clm_file.seek(k*page_size);
				
				davisbase_clm_file.skipBytes(2);
				int no_of_records = (int) davisbase_clm_file.readShort();
				davisbase_clm_file.skipBytes(12);
				
				int[] record_offset = new int[no_of_records];
				
				for(int s = 0; s < no_of_records; s++){
				
					record_offset[s] = davisbase_clm_file.readShort();
					
				}
				
				
				
				for(int r = 0; r < no_of_records; r++){
					
					String[] values = null;
					values = Initial_Prompt_File.retrieveValues(davisbase_clm_file, record_offset[r]);
					
					if( (values[1].equals(table_name.trim())) && (values[2].equals(col_name.trim()))){
						
						if (values[3].contains("varchar")){
								datatype = "text";
						}
						else if (values[3].contains("decimal")){
								datatype = "float";
						}
						else {
						datatype = values[3];
						}
						break;
					}
					
				}
			}
		}
		catch(Exception e){
			System.out.println(e);	
		}
		int code = getDatatypeCode(datatype.toUpperCase());
		int[] return_val = {getDatatypeSize(datatype.toUpperCase()), code};
		return return_val;
	}
	
	public static int getOrdinalPosition(String table_name, String col_name){
	
		String working_dir = System.getProperty("user.dir");
		String catalog_path = working_dir + '\\' + output_folder + '\\' + catalog_folder;
		String filename = column_catalog + ".tbl";
		String datatype = "";
		int pos = 0;
		
		try
		{
			RandomAccessFile davisbase_clm_file = new RandomAccessFile(catalog_path +"\\"+ filename , "rw");
			int no_of_pages = Commands.getPageCount(davisbase_clm_file);
			
			
			
			for( int k = 0; k < no_of_pages; k++){
			
				 
				davisbase_clm_file.seek(k*page_size);
				
				davisbase_clm_file.skipBytes(2);
				int no_of_records = (int) davisbase_clm_file.readShort();
				davisbase_clm_file.skipBytes(12);
				
				int[] record_offset = new int[no_of_records];
				
				for(int s = 0; s < no_of_records; s++){
				
					record_offset[s] = davisbase_clm_file.readShort();
					
				}
				
				
				
				for(int r = 0; r < no_of_records; r++){
					
					String[] values = null;
					values = Initial_Prompt_File.retrieveValues(davisbase_clm_file, record_offset[r]);
					
					if( (values[1].equals(table_name.trim())) && (values[2].equals(col_name.trim()))){
						pos = Integer.parseInt(values[4]);	
						break;
					}
				}
			}
		}
		catch(Exception e){
			System.out.println(e);	
		}
		return pos;
	}
	
	public static int getDatatypeSize(String datatype){
		
		switch(datatype.toUpperCase()){
				case "TINYINT":     return 1;
				case "SMALLINT":    return 2;
				case "INT":			return 4;
				case "BIGINT":      return 8;
				case "LONG":        return 8;
				case "FLOAT":       return 4;
				case "DOUBLE":      return 8;
				case "DATETIME":    return 8;
				case "DATE":        return 8;
				case "YEAR":        return 1;
				case "TEXT":        return 12;
				default:			return 0;
		
		
	}	
	}
	
	public static int getDatatypeCode(String datatype){
		
		switch(datatype.toUpperCase()){
				case "TINYINT":     return 1;
				case "SMALLINT":    return 2;
				case "INT":			return 3;
				case "BIGINT":      return 4;
				case "LONG":        return 4;
				case "FLOAT":       return 5;
				case "DOUBLE":      return 6;
				case "DATETIME":    return 10;
				case "DATE":        return 11;
				case "YEAR":        return 8;
				case "TEXT":        return 12;
			    default:			return 0;
		}
	}
	
	
	public static void parseDelete(ArrayList<String> commandTokens) {
		System.out.println("Command: " + tokensToCommandString(commandTokens));
		System.out.println("Stub: This is the deleteRecord method");
		/* TODO: Your code goes here */
	}
	

	/**
	 *  Stub method for dropping tables
	 */
	public static void parseDropTable(String usercommand) {
		System.out.println("Command: " + usercommand);
		System.out.println("Stub: This is the dropTable method.");
		
		ArrayList<String> commandTokens = commandStringToTokenList(usercommand);
		
		/* Extract the table name from the command string token list */
		String tablename = commandTokens.get(2);
		String tableFileName = commandTokens.get(2) + ".tbl";
		String working_dir = System.getProperty("user.dir");
		String data_path = working_dir + '\\' + Initial_Prompt_File.output_folder + '\\' + Initial_Prompt_File.data_folder;
		
		//Method to check if the file already exists
		if(Initial_Prompt_File.checkFileExists(data_path, tableFileName)){
			try {
				System.out.println("File exists, Deleting...");
				File file_pointer = new File(data_path+'\\'+tableFileName); 
				if (file_pointer.delete()) {      
				  System.out.println("Deleted the file: " + file_pointer.getName());
				} else {
				  System.out.println("Failed to delete the file.");
				}
				//file_pointer.close();
		        }
		    catch(Exception e){
		    	System.out.println(e);	
		    }
		    try{
		    	String catalog_path = working_dir + '\\' + output_folder + '\\' + catalog_folder;
		    	String filename = table_catalog + ".tbl";
		    	RandomAccessFile davisbase_tbl_file = new RandomAccessFile(catalog_path +"\\"+ filename , "rw");
		    	
		    	int no_of_pages = Commands.getPageCount(davisbase_tbl_file);
			
			
			
			for( int k = 0; k < no_of_pages; k++){
			
				 
				davisbase_tbl_file.seek(k*page_size);
				
				davisbase_tbl_file.skipBytes(2);
				int no_of_records = (int) davisbase_tbl_file.readShort();
				davisbase_tbl_file.skipBytes(12);
				
				int[] record_offset = new int[no_of_records];
				
				for(int s = 0; s < no_of_records; s++){
				
					record_offset[s] = davisbase_tbl_file.readShort();
					
				}
				
				
				for(int r = 0; r < no_of_records; r++){
					
					String[] values = null;
					values = Initial_Prompt_File.retrieveValues(davisbase_tbl_file, record_offset[r]);
					
					if(values[1].equals(tablename)) {
						
							davisbase_tbl_file.seek(record_offset[r]);
							int payload = davisbase_tbl_file.readShort();
							davisbase_tbl_file.seek(k*page_size);
							davisbase_tbl_file.skipBytes(2);
							int fp = (int)davisbase_tbl_file.getFilePointer();
							int no_of_cells = davisbase_tbl_file.readShort();
							davisbase_tbl_file.seek(fp);
							davisbase_tbl_file.writeShort(no_of_cells-1);
							davisbase_tbl_file.skipBytes(12);
							davisbase_tbl_file.skipBytes((no_of_cells-2)*2);
							int old_cell = (int)davisbase_tbl_file.getFilePointer();
							davisbase_tbl_file.skipBytes(2);
							davisbase_tbl_file.writeShort(0x00);
							davisbase_tbl_file.seek(fp+2);
							davisbase_tbl_file.writeShort(old_cell);
							
							davisbase_tbl_file.seek(record_offset[r]);
							payload += 6;
							byte b[] = new byte[payload];
							davisbase_tbl_file.write(b);
							
							break;
						
					}
					
				}
			}
		    }
		    catch(Exception e)
		    {
		    	System.out.println(e);	
		    }
		}
	}

	/**
	 *  Stub method for executing queries
	 */
	public static void parseSelect(String command) {
		
		
		ArrayList<String> commandTokens = commandStringToTokenList(command);
		
		System.out.println("Command: " + command);
		System.out.println("Stub: This is the parseSelect method");
		
		/* write code to display all values*/
		int index = commandTokens.indexOf("from");
		String tablename = commandTokens.get(index+1);
		String tableFileName = tablename + ".tbl";
		System.out.println(tableFileName);
		String working_dir = System.getProperty("user.dir");
		String data_path = working_dir + '\\' + output_folder + '\\' + data_folder;
		
		if(Initial_Prompt_File.checkFileExists(data_path, tableFileName)){
			try {
				RandomAccessFile tablefile = new RandomAccessFile(data_path +"\\"+  tableFileName, "rw");
				int no_of_pages = getPageCount(tablefile);
				
				
				/*String[] col_split = (command_iter.split("from")[0]).split(",").trim();
				
				SELECT *
				FROM Dogs
				WHERE tag_id = 9090;*/
				String command_iter = "";
				String[] cond_split = new String[5	];
				String cond_col ="";
				int cond_value = 0;
				int pos = 0;
				//for(String i: col_split){System.out.println(i);} 
				if(command.contains("where")){
					command_iter = command.replace("select","");
					cond_split = (command_iter.split("where")[1]).trim().split(" ");
					//for(String i: cond_split){System.out.println(i);}
					cond_col = cond_split[0];
					System.out.println("Cond_col: "+cond_col);
					cond_value = Integer.parseInt(cond_split[2].trim()); 
					System.out.println("Cond_Val: "+cond_value);
					System.out.println("Cond_operator: "+ cond_split[1]);
					pos = getOrdinalPosition(tablename, cond_col);
					System.out.println("Ordinal Position: " +pos);
					
					if(command.contains("or") || command.contains("and"))
					{
						
					}
					
				}
				
			    for( int k = 0; k < no_of_pages; k++){
			
					tablefile.seek(k*page_size);
					
					tablefile.skipBytes(2);
					int no_of_records = (int) tablefile.readShort();
					tablefile.skipBytes(12);
					
					int[] record_offset = new int[no_of_records];
					
					for(int s = 0; s < no_of_records; s++){
					
						record_offset[s] = tablefile.readShort();
						
					}
				
				for(int r = 0; r < no_of_records; r++){
					
					String[] values = null;
					values = Initial_Prompt_File.retrieveValues(tablefile, record_offset[r]);
					if(command.contains("where")){
						if(cond_split[1].equals("=")){
							if((Integer.parseInt(values[pos])) == cond_value){
								for(String i: values){
								System.out.print(String.format("%-20s",i)+"|");
								}
								System.out.println();
							}
						}
						if(cond_split[1].equals(">=")){
							if((Integer.parseInt(values[pos])) >= cond_value){
								for(String i: values){
								System.out.print(String.format("%-20s",i)+"|");
								}
								System.out.println();
							}
						}
						if(cond_split[1].equals("<=")){
							if((Integer.parseInt(values[pos])) <= cond_value){
								for(String i: values){
								System.out.print(String.format("%-20s",i)+"|");
								}
								System.out.println();
							}
						}
					}
					else{
						for(String i: values){
								System.out.print(String.format("%-20s",i)+"|");}	
								System.out.println();
					}
				}
				}
					
				
				tablefile.close();
		
		
		}catch(Exception e){System.out.println(e);}
		
		
	}
	}
	

	/**
	 *  Stub method for updating records
	 *  @param updateString is a String of the user input
	 */
	public static void parseUpdate(ArrayList<String> commandTokens) {
		System.out.println("Command: " + tokensToCommandString(commandTokens));
		System.out.println("Stub: This is the parseUpdate method");
	}

	public static String tokensToCommandString (ArrayList<String> commandTokens) {
		String commandString = "";
		for(String token : commandTokens)
			commandString = commandString + token + " ";
		return commandString;
	}
	
	public static ArrayList<String> commandStringToTokenList (String command) {
		command.replace("\n", " ");
		command.replace("\r", " ");
		command.replace(",", " , ");
		command.replace("\\(", " ( ");
		command.replace("\\)", " ) ");
		ArrayList<String> tokenizedCommand = new ArrayList<String>(Arrays.asList(command.split(" ")));
		return tokenizedCommand;
	}

	public static void UpdateDavisbaseTables(String tablename){
		
		String working_dir = System.getProperty("user.dir");
		String catalog_path = working_dir + '\\' + Initial_Prompt_File.output_folder + '\\' + Initial_Prompt_File.catalog_folder;
		String filename = Initial_Prompt_File.table_catalog + ".tbl";
		
		//System.out.println("Inside Update DavisBase Tables");
		
		try{
			//Check if space is available to update the davisbasetable file	
			
			
			insertInto(catalog_path, filename, tablename);
		}
		catch(Exception e){
			
		}
	}
		
	public static void insertInto(String dir_path, String filename, String table_name){
		
		String filename_sub = filename.substring(0,filename.length()-4);
		
		if(filename_sub.equals(Initial_Prompt_File.table_catalog)){
			
			try{
				RandomAccessFile davisbase_tbl_file = new RandomAccessFile(dir_path +"\\"+ filename , "rw");
				int root_page = 0;
				int record_count = 0;
				int avg_record = 0;
			
				System.out.println("Insert Into Davisbase Table : "+table_name);
				table_name = table_name.substring(0,table_name.length()-4); //removes .tbl
				
				
				int offset = payload_content_size + row_id_size + 5 + table_name.length() + 3;
				int payload = offset - 6;
			
    			/*If space is available in the last page, add record. Else add page */
				int no_of_pages = getPageCount(davisbase_tbl_file);
				
				if(!checkByteAvailability(davisbase_tbl_file)){
					/*Not enough space in the current page, hence create a new page*/
					
					davisbase_tbl_file.setLength((no_of_pages+1)*page_size);
					
					davisbase_tbl_file.seek((no_of_pages-1)* page_size);
					davisbase_tbl_file.skipBytes(6);
					davisbase_tbl_file.writeInt(no_of_pages+1); // right Sibling
					if (no_of_pages!=1){davisbase_tbl_file.writeInt(no_of_pages-1);}
					
					davisbase_tbl_file.seek(no_of_pages* page_size);
					davisbase_tbl_file.writeByte(0x0D); //One byte flag to identify page type
					davisbase_tbl_file.skipBytes(2);
					davisbase_tbl_file.writeShort(0x00);
					davisbase_tbl_file.writeShort(no_of_pages* page_size);
					davisbase_tbl_file.writeInt(0xffffffff); //No right Sibling
					davisbase_tbl_file.writeInt(0xffffffff); //NO parent
				}
					
				/*Go to the last Davisbase page and add the record*/
				no_of_pages = getPageCount(davisbase_tbl_file);
				davisbase_tbl_file.seek((no_of_pages-1)* page_size);
				davisbase_tbl_file.skipBytes(2);
				int fp = (int)davisbase_tbl_file.getFilePointer();
				int no_of_cells = (int) davisbase_tbl_file.readShort(); //retrieve rowid
				davisbase_tbl_file.seek(fp);
				davisbase_tbl_file.writeShort(no_of_cells+1); //update row
					
				fp = (int)davisbase_tbl_file.getFilePointer();
				int content_begins = (int) davisbase_tbl_file.readShort();
				davisbase_tbl_file.seek(content_begins);
				davisbase_tbl_file.skipBytes(2); //skip payload
			    int row_no = davisbase_tbl_file.readInt(); // last row no
				int new_content_begins = content_begins - offset;
				davisbase_tbl_file.seek(fp);
					
				davisbase_tbl_file.writeShort(new_content_begins);
				davisbase_tbl_file.skipBytes(10);
					
				davisbase_tbl_file.skipBytes(no_of_cells*2);
				davisbase_tbl_file.writeShort(new_content_begins); 
				davisbase_tbl_file.seek(new_content_begins);
					
				davisbase_tbl_file.writeShort(payload);
				davisbase_tbl_file.writeInt(row_no+1);
				davisbase_tbl_file.writeByte(0x04); //contains four columns
				davisbase_tbl_file.writeByte(table_name.length()+12);
				davisbase_tbl_file.writeByte(1); //root page is 
				davisbase_tbl_file.writeByte(1); //record count size
				davisbase_tbl_file.writeByte(1); //avg_record size
				davisbase_tbl_file.writeBytes(table_name);
				davisbase_tbl_file.writeByte(0x00);
				davisbase_tbl_file.writeByte(0x00);
				davisbase_tbl_file.writeByte(0x00);
				
				davisbase_tbl_file.close();
			}
			catch(Exception e){
				System.out.println(e);
			}
				
		}
		
	}
	
	public static int getPageCount(RandomAccessFile file){
		
		int num_pages = 0;
		
		
		try{
			int file_length = (int)file.length();
			num_pages = file_length/page_size;
		}
		catch(Exception e){
			System.out.println(e);	
		}
		//System.out.println(num_pages);
		return num_pages;
		
	}
	
	public static boolean checkByteAvailability(RandomAccessFile file){
		//go to the last page and find the last record offset and the page header
		
		int byte_difference = 0;
		
		boolean space_flag = false;
		
		try{
			int page_count = getPageCount(file);
			file.seek((page_count-1)* page_size); //go to the last page
			//System.out.println(file.getFilePointer());
			file.skipBytes(2);
			//System.out.println(file.getFilePointer());
			int no_of_cells = (int) file.readShort();
			if(no_of_cells == 0){ 
				return !space_flag;
			}
			
			//System.out.println("Cells: "+no_of_cells);
			file.skipBytes(12);
			int last_cell = 2 *(no_of_cells - 1);
			//System.out.println(last_cell);
			file.skipBytes(last_cell);
			//System.out.println("File pointer of last element:"+ file.getFilePointer());
			//System.out.println(file.readShort());
			//System.out.println(file.getFilePointer());
			
			byte_difference = (int)file.readShort() - (int)file.getFilePointer();
		}
		catch(Exception e){
			System.out.println(e);	
		}
		//check if the bytes available is atleast 10% of the page_size
		if(byte_difference >= (0.1)*page_size){
			return !space_flag;
		}
		else{
			return space_flag;
		}
	}
	
	public static void updateDavisbaseColumns(String table_name, String col_name, String datatype, int pos, boolean nullable, boolean unique){
	
		String working_dir = System.getProperty("user.dir");
		String catalog_path = working_dir + '\\' + Initial_Prompt_File.output_folder + '\\' + Initial_Prompt_File.catalog_folder;
		String filename = Initial_Prompt_File.column_catalog + ".tbl";
		
		//System.out.println("Inside Update DavisBase Tables");
		
		try{
			//Check if space is available to update the davisbasetable file	
			 RandomAccessFile davisbase_clm_file = new RandomAccessFile(catalog_path +"\\"+ filename , "rw");
			
			 int no_of_pages = getPageCount(davisbase_clm_file);
			 int offset = payload_content_size + row_id_size + 7 +table_name.length() + col_name.length() + datatype.length()+ 1;
			 
			 String null_value ="";
			 String unique_value ="";
			 
			 if (nullable == true){ 
			 	offset += 3;
			    null_value = "YES";
			 }
			 else { 
			 	 offset +=2;
			 	 null_value = "NO";
			 }
			 if (unique == true){ 
			 	 offset += 3;
			 	 unique_value = "YES";
			 }
			 else { 
			 	offset +=2;
			 	unique_value = "NO";
			 }
			 
			 int payload = offset - 6;
			 boolean new_page = false;
			 int prev_row_no =0;
			 
			 if(!checkByteAvailability(davisbase_clm_file)){
			 	 
			 	new_page = true;
			 	davisbase_clm_file.setLength((no_of_pages+1)*page_size);
					
				davisbase_clm_file.seek((no_of_pages-1)* page_size);
				davisbase_clm_file.skipBytes(4);
				int prev_content = (int) davisbase_clm_file.readShort();
				davisbase_clm_file.seek(prev_content);
				davisbase_clm_file.skipBytes(2); //skip payload
				prev_row_no = davisbase_clm_file.readInt();
				davisbase_clm_file.seek((no_of_pages-1)* page_size);
				davisbase_clm_file.skipBytes(6);
				davisbase_clm_file.writeInt(no_of_pages+1); // right Sibling
				if (no_of_pages!=1){davisbase_clm_file.writeInt(no_of_pages-1);}
				

				davisbase_clm_file.seek(no_of_pages* page_size);
				davisbase_clm_file.writeByte(0x0D); //One byte flag to identify page type
				davisbase_clm_file.skipBytes(2);
				davisbase_clm_file.writeShort(0x00);
				davisbase_clm_file.writeShort(no_of_pages* page_size);
				davisbase_clm_file.writeInt(0xffffffff); //No right Sibling
				davisbase_clm_file.writeInt(0xffffffff); //NO parent
			 	 
			 }
			 
			 no_of_pages = getPageCount(davisbase_clm_file);
			 davisbase_clm_file.seek((no_of_pages-1)* page_size);
			 davisbase_clm_file.skipBytes(2);
			 int fp = (int)davisbase_clm_file.getFilePointer();
			 int no_of_cells = (int) davisbase_clm_file.readShort(); //retrieve row nos
			 davisbase_clm_file.seek(fp);
			 davisbase_clm_file.writeShort(no_of_cells+1); //update row nos
					
			 fp = (int)davisbase_clm_file.getFilePointer();
			 int content_begins = (int) davisbase_clm_file.readShort();
			 davisbase_clm_file.seek(content_begins);
			 davisbase_clm_file.skipBytes(2); //skip payload
			 int row_no = davisbase_clm_file.readInt(); // last row no
			 int new_content_begins = content_begins - offset;
			 davisbase_clm_file.seek(fp);
			 davisbase_clm_file.writeShort(new_content_begins);
			 davisbase_clm_file.skipBytes(10);
			 davisbase_clm_file.skipBytes(no_of_cells*2);
			 davisbase_clm_file.writeShort(new_content_begins);
			 
			 davisbase_clm_file.seek(new_content_begins);
			 davisbase_clm_file.writeShort(payload);
			 if (new_page){ row_no = prev_row_no; }
			 davisbase_clm_file.writeInt(row_no+1);
			 davisbase_clm_file.writeByte(6);
			 davisbase_clm_file.writeByte(table_name.length()+12);
			 davisbase_clm_file.writeByte(col_name.length()+12);
			 davisbase_clm_file.writeByte(datatype.length()+12);
			 davisbase_clm_file.writeByte(1);
			 davisbase_clm_file.writeByte(null_value.length()+12);
			 davisbase_clm_file.writeByte(unique_value.length()+12);
			 davisbase_clm_file.writeBytes(table_name);
			 davisbase_clm_file.writeBytes(col_name);
			 davisbase_clm_file.writeBytes(datatype);
			 davisbase_clm_file.writeByte(pos);
			 davisbase_clm_file.writeBytes(null_value);
			 davisbase_clm_file.writeBytes(unique_value);
			 
			 davisbase_clm_file.close();
		}
		catch(Exception e){
			System.out.println(e);
		}
	}	
	}
	
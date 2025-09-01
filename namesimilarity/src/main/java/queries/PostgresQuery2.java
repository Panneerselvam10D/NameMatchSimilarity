package queries;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class PostgresQuery2 {

    private static final String URL = "jdbc:postgresql://38.242.220.73:5433/postgres";
    private static final String USER = "postgres";
    private static final String PASSWORD = "admin123";

    public static void main(String[] args) {



        String sql = "WITH q AS ( " +
                "  SELECT websearch_to_tsquery('simple', unaccent(?)) AS tsq " +
                "), docs AS ( " +
                "  SELECT sanction_id, sdnname, to_tsvector('simple', unaccent(sdnname)) AS doc " +
                "  FROM sanction_active " +
                ") " +
                "SELECT sanction_id, sdnname, ts_rank_cd(doc, q.tsq, 32) AS score " +
                "FROM docs, q " +
                "WHERE doc @@ q.tsq " +
                "ORDER BY score DESC;";

        
//
//        String sql =
//        	    "WITH input_tokens AS ( " +
//        	    "    SELECT unnest(string_to_array(lower(?), ' ')) AS token " +
//        	    "), matched AS ( " +
//        	    "    SELECT sa.sdnname, " +
//        	    "           sa.sanction_id, " +
//        	    "           sa.sdnname_tokens_count, " +
//        	    "           i.token AS input_token, " +
//        	    "           sdn_token, " +
//        	    "           levenshtein(sdn_token, i.token) AS distance " +
//        	    "    FROM sanction_active sa " +
//        	    "    JOIN LATERAL unnest(sa.sdnname_tokens) AS sdn_token ON TRUE " +
//        	    "    JOIN input_tokens i ON levenshtein(sdn_token, i.token) < 3 " +
//        	    "), aggregated AS ( " +
//        	    "    SELECT sdnname, " +
//        	    "           sanction_id, " +
//        	    "           sdnname_tokens_count, " +
//        	    "           COUNT(DISTINCT input_token) AS matched_input_tokens, " +
//        	    "           (sdnname_tokens_count + 2) - (COUNT(DISTINCT input_token) * 2) AS score_calc " +
//        	    "    FROM matched " +
//        	    "    GROUP BY sdnname, sanction_id, sdnname_tokens_count " +
//        	    "    HAVING (sdnname_tokens_count + 2) - (COUNT(DISTINCT input_token) * 2) <= 4 " +
//        	    ") " +
//        	    "SELECT sdnname, " +
//        	    "       sanction_id, " +
//        	    "       sdnname_tokens_count, " +
//        	    "       matched_input_tokens, " +
//        	    "       score_calc " +
//        	    "FROM aggregated " +
//        	    "ORDER BY matched_input_tokens DESC;";


        

//        String sql =
//            "WITH q AS ( " +
//            "  SELECT regexp_replace(unaccent(lower(?)), '\\s+', ' ', 'g') AS qtxt " +
//            "), s AS ( " +
//            "  SELECT sdnname,sanction_id, " +
//            "         regexp_replace(unaccent(lower(sdnname)), '\\s+', ' ', 'g') AS nrm " +
//            "  FROM sanction_active " +
//            "  WHERE \"type\" = 'Person' " +
//            "), m AS ( " +
//            "  SELECT s.sdnname,s.sanction_id, " +
//            "         levenshtein(s.nrm, q.qtxt) AS d, " +
//            "         GREATEST(length(s.nrm), length(q.qtxt)) AS denom " +
//            "  FROM s, q " +
//            "), a AS ( " +
//            "    SELECT " +
//            "      sdnname,sanction_id, " +
//            "      d AS levenshtein_distance, " +
//            "      ROUND((1 - d::numeric / NULLIF(denom,0)) * 100, 2) AS similarity_percent " +
//            "    FROM m " +
//            ") " +
//            "SELECT " +
//            "    a.sdnname,a.sanction_id, a.levenshtein_distance, a.similarity_percent " +
//            "FROM a " +
//            "WHERE a.similarity_percent > 40 " +
//            "ORDER BY similarity_percent DESC;";
        
    	
//      String inputName = "Oleksander Kostyantynovych Akimov";      
        
		String InputExcelFilePath = "/home/decoders/Music/CLIENT_60.xlsx";

		
		

		try {
			FileInputStream fis = new FileInputStream(new File(InputExcelFilePath));
			Workbook workbook = new XSSFWorkbook(fis);
			Sheet sheet = workbook.getSheetAt(0); 
			Integer times = 1;
			for (Row row : sheet) {
				Cell cell = row.getCell(0); 
				if (cell != null) {
					String inputName = cell.getStringCellValue().trim();
					System.out.println("Processing: " + inputName);
//					if(times > 10) {
//						break;
//					}

					checkSimiliarity(inputName,sql);
					times++;
				}
			}

			workbook.close();
			fis.close();
			
			System.out.println("Neo4j Results Write Completed In Excel");

		} catch (IOException e) {
			e.printStackTrace();
		}
  
    }
    
    
    
	private static void checkSimiliarity(String inputName, String sql) {

		String filePath = "/home/decoders/Music/matched_results.xlsx";
		List<String[]> results = new ArrayList<>();

		try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
				PreparedStatement stmt = conn.prepareStatement(sql)) {

			stmt.setString(1, inputName);

			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					String sdnname = rs.getString("sdnname");
					String sanctionId = rs.getString("sanction_id");

					results.add(new String[] { sanctionId, sdnname });
				}
			}

			System.out.println("Query Execution Completed");

		} catch (SQLException e) {
			e.printStackTrace();
		}

		writeResultsToExcel(filePath, inputName, results);

	}

	
	
    private static void writeResultsToExcel(String filePath, String sheetName, List<String[]> results) {
        try (FileInputStream fis = new FileInputStream(filePath);
             XSSFWorkbook workbook = new XSSFWorkbook(fis)) {
        	
        	sheetName = sheetName.length() > 31 ? sheetName.substring(0, 31) : sheetName;

            Sheet sheet = workbook.getSheet(sheetName);
            if (sheet == null) {
                sheet = workbook.createSheet(sheetName);
            }

            Row header = sheet.getRow(0);
            if (header == null) {
                header = sheet.createRow(0);
            }
            if (header.getCell(3) == null) header.createCell(3).setCellValue("Sanction ID - Postgres");
            if (header.getCell(4) == null) header.createCell(4).setCellValue("SDN Name - Postgres");
            if (header.getCell(5) == null) header.createCell(5).setCellValue("Compare B with E"); // New column F

            int rowNum = 1;
            for (String[] rowData : results) {
                Row row = sheet.getRow(rowNum);
                if (row == null) {
                    row = sheet.createRow(rowNum);
                }
                row.createCell(3).setCellValue(rowData[0]); 
                row.createCell(4).setCellValue(rowData[1]); 

                Cell formulaCell = row.createCell(5);
                String formula = "COUNTIF(E:E,B" + (rowNum + 1) + ")>0";
                formulaCell.setCellFormula(formula);

                rowNum++;
            }

            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                workbook.write(fos);
            }

            System.out.println("Postgres results written to Excel with compare column: " + filePath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

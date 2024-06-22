/***********************************************************************
 * This file is part of iDempiere ERP Open Source                      *
 * http://www.idempiere.org                                            *
 *                                                                     *
 * Copyright (C) Contributors                                          *
 *                                                                     *
 * This program is free software; you can redistribute it and/or       *
 * modify it under the terms of the GNU General Public License         *
 * as published by the Free Software Foundation; either version 2      *
 * of the License, or (at your option) any later version.              *
 *                                                                     *
 * This program is distributed in the hope that it will be useful,     *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of      *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the        *
 * GNU General Public License for more details.                        *
 *                                                                     *
 * You should have received a copy of the GNU General Public License   *
 * along with this program; if not, write to the Free Software         *
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,          *
 * MA 02110-1301, USA.                                                 *
 *                                                                     *
 * Contributors:                                                       *
 * - Carlos Ruiz - globalqss - bxservice                               *
 **********************************************************************/

/**
 *
 * @author Carlos Ruiz - globalqss - bxservice
 *
 */
package de.bxservice.validatefilesystemstorageprovider.process;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.compiere.model.MArchive;
import org.compiere.model.MAttachment;
import org.compiere.model.MProcessPara;
import org.compiere.model.MTable;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;

/**
 * Validate file system storage providers for problems with missing or orphan files
 */
public class CheckFiles extends SvrProcess{

	final static private BigDecimal TWO = BigDecimal.valueOf(2.0);
	final static private BigDecimal THREE = BigDecimal.valueOf(3.0);
	final static private BigDecimal FOUR = BigDecimal.valueOf(4.0);

	@Override
	protected void prepare() {
		for (ProcessInfoParameter para : getParameter()) {
			String name = para.getParameterName();
			switch (name) {
			default:
				MProcessPara.validateUnknownParameter(getProcessInfo().getAD_Process_ID(), para);
			}
		}
	}

	@Override
	protected String doIt() throws Exception {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		sql.append("  REPLACE( ");
		sql.append("  REPLACE( ");
		sql.append("  REPLACE( ");
		sql.append("    filename ");
		sql.append("    ,'&amp;', '&') ");
		sql.append("    ,'&lt;', '<') ");
		sql.append("    ,'&gt;', '>') ");
		sql.append("    AS filename, ad_table_id, record_id, tablename ");
		sql.append("FROM ( ");
		sql.append("SELECT ");
		sql.append("	REPLACE( ");
		sql.append("	    UNNEST( ");
		sql.append("	      XPATH('//attachments/entry/@file', ");
		sql.append("	          CONVERT_FROM(a.binarydata, 'UTF-8')::xml ");
		sql.append("	           ))::varchar ");
		sql.append("	        , '%ATTACHMENT_FOLDER%', s.folder||'/') AS filename ");
		sql.append("    , a.ad_table_id, a.record_id, 'AD_Attachment' AS tablename ");
		sql.append("FROM ad_attachment a ");
		sql.append("  JOIN ad_storageprovider s ON (a.ad_storageprovider_id=s.ad_storageprovider_id AND s.METHOD='FileSystem') ");
		sql.append("WHERE a.title = 'xml' ");
		sql.append("  AND a.binarydata IS NOT NULL ");
		if (getAD_Client_ID() > 0)
			sql.append("  AND a.ad_client_id=? ");
		sql.append("UNION ");
		sql.append("SELECT ");
		sql.append("	REPLACE( ");
		sql.append("	    UNNEST( ");
		sql.append("	      XPATH('//archive/entry/@file', ");
		sql.append("	          CONVERT_FROM(a.binarydata, 'UTF-8')::xml ");
		sql.append("	           ))::varchar ");
		sql.append("	        , '%ARCHIVE_FOLDER%', s.folder||'/') AS filename ");
		sql.append("    , a.ad_table_id, a.record_id, 'AD_Archive' ");
		sql.append("FROM ad_archive a ");
		sql.append("  JOIN ad_storageprovider s ON (a.ad_storageprovider_id=s.ad_storageprovider_id AND s.METHOD='FileSystem') ");
		sql.append("WHERE a.binarydata IS NOT null ");
		if (getAD_Client_ID() > 0)
			sql.append("  AND a.ad_client_id=? ");
		sql.append("UNION ");
		sql.append("SELECT ");
		sql.append("	REPLACE( ");
		sql.append("	    UNNEST( ");
		sql.append("	      XPATH('//image/entry/@file', ");
		sql.append("	          CONVERT_FROM(a.binarydata, 'UTF-8')::xml ");
		sql.append("	           ))::varchar ");
		sql.append("	        , '%IMAGE_FOLDER%', s.folder||'/') AS filename ");
		sql.append("    , 0, 0, 'AD_Image' ");
		sql.append("FROM ad_image a ");
		sql.append("  JOIN ad_storageprovider s ON (a.ad_storageprovider_id=s.ad_storageprovider_id AND s.METHOD='FileSystem') ");
		sql.append("WHERE a.binarydata IS NOT null ");
		if (getAD_Client_ID() > 0)
			sql.append("  AND a.ad_client_id=? ");
		sql.append(") f ");
		sql.append("ORDER BY 1");

		List<List<Object>> records;
		if (getAD_Client_ID() > 0)
			records = DB.getSQLArrayObjectsEx(get_TrxName(), sql.toString(), getAD_Client_ID(), getAD_Client_ID(), getAD_Client_ID());
		else
			records = DB.getSQLArrayObjectsEx(get_TrxName(), sql.toString());
		List<String> allFiles = new ArrayList<String>();
		List<List<Object>> missingFiles = new ArrayList<List<Object>>();
		List<List<Object>> emptyFiles = new ArrayList<List<Object>>();
		List<List<Object>> orphanRecords = new ArrayList<List<Object>>();
		for (List<Object> record : records) {
			if (allFiles.size() % 1000 == 0)
				statusUpdate("Processing ... " + allFiles.size() + " / " + records.size());
			String filename = record.get(0).toString();
			allFiles.add(filename);

			File file = new File(filename);
			if (! file.exists())
				missingFiles.add(record);
			else if (file.length() == 0)
				emptyFiles.add(record);

			int tableId  = 0;
			if (record.get(1) != null && record.get(1) instanceof BigDecimal)
				tableId = ((BigDecimal) record.get(1)).intValue();
			int recordId = 0;
			if (record.get(2) != null && record.get(2) instanceof BigDecimal)
				recordId = ((BigDecimal) record.get(2)).intValue();

			if (recordId > 0) {
				ArrayList<Object> tabRec = new ArrayList<Object>();
				tabRec.add(null); // clear the fileName
				tabRec.add(record.get(1));
				tabRec.add(record.get(2));
				tabRec.add(record.get(3));
				if (! existInArray(orphanRecords, tabRec)) {
					MTable table = MTable.get(tableId);
					if (table.getKeyColumns().length == 1) {
						StringBuilder verifSql = new StringBuilder("SELECT 1 FROM ")
								.append(table.getTableName())
								.append(" WHERE ")
								.append(table.getKeyColumns()[0])
								.append("=?");
						int one = DB.getSQLValueEx(get_TrxName(), verifSql.toString(), recordId);
						if (one != 1)
							orphanRecords.add(tabRec);
					} else {
						orphanRecords.add(tabRec); // table without ID column, or with multi-ID, the Record_ID is wrong
					}
				}
			}
		}

		// traverse attachment/archive folders and verify if the file is listed in the list
		StringBuilder  sqlFolders = new StringBuilder();
		sqlFolders.append("SELECT DISTINCT Folder ");
		sqlFolders.append("FROM   AD_StorageProvider sp ");
		sqlFolders.append("       JOIN AD_ClientInfo ci ");
		sqlFolders.append("         ON ( ci.AD_StorageProvider_ID = sp.AD_StorageProvider_ID ");
		sqlFolders.append("               OR ci.StorageArchive_ID = sp.AD_StorageProvider_ID ) ");
		sqlFolders.append("WHERE  sp.Method = 'FileSystem' ");
		if (getAD_Client_ID() > 0)
			sqlFolders.append("   AND ci.AD_Client_ID=").append(getAD_Client_ID());
		sqlFolders.append(" UNION ");
		sqlFolders.append("SELECT DISTINCT Folder ");
		sqlFolders.append("                || '%AD_Image%' ");
		sqlFolders.append("FROM   AD_StorageProvider sp ");
		sqlFolders.append("       JOIN AD_ClientInfo ci ");
		sqlFolders.append("         ON ( ci.StorageImage_ID = sp.AD_StorageProvider_ID ) ");
		sqlFolders.append("WHERE  sp.Method = 'FileSystem' ");
		if (getAD_Client_ID() > 0)
			sqlFolders.append("   AND ci.AD_Client_ID=").append(getAD_Client_ID());
		sqlFolders.append(" ORDER  BY 1");

		Set<String> allFilesSet = new HashSet<>(allFiles);
		List<String> orphanFiles = new ArrayList<String>();
		List<List<Object>> folders = DB.getSQLArrayObjectsEx(get_TrxName(), sqlFolders.toString());
		for (List<Object> folder : folders) {
			String folderPath = folder.get(0).toString();
			if (folderPath.endsWith("%AD_Image%")) {
				folderPath = folderPath.substring(0, folderPath.length()-10);
				if (! folderPath.endsWith(File.separator))
					folderPath += File.separator;
				folderPath += "AD_Image" + File.separator;
			}
			if (getAD_Client_ID() > 0) {
				if (! folderPath.endsWith(File.separator))
					folderPath += File.separator;
				folderPath += getAD_Client_ID();
			}
			Files.walk(Paths.get(folderPath))
			.filter(Files::isRegularFile)
			.forEach(path -> {
				String fileName = path.toString();
				if (! allFilesSet.contains(fileName))
					orphanFiles.add(fileName);
			});
		}

		// report lists
		if (missingFiles.size() > 0) {
			addLog("** Missing Files **");
			for (List<Object> record : missingFiles) {
				String fileName = record.get(0).toString();
				int tableId  = 0;
				if (record.get(1) != null && record.get(1) instanceof BigDecimal)
					tableId = ((BigDecimal) record.get(1)).intValue();
				int recordId = 0;
				if (record.get(2) != null && record.get(2) instanceof BigDecimal)
					recordId = ((BigDecimal) record.get(2)).intValue();
				if (tableId > 0 && recordId > 0)
					addLog(recordId, null, Env.ONE, fileName, tableId, recordId);
				else
					addLog (0, null, Env.ONE, fileName);
			}
		}

		if (emptyFiles.size() > 0) {
			addLog("** Empty Files **");
			for (List<Object> record : emptyFiles) {
				String fileName = record.get(0).toString();
				int tableId  = 0;
				if (record.get(1) != null && record.get(1) instanceof BigDecimal)
					tableId = ((BigDecimal) record.get(1)).intValue();
				int recordId = 0;
				if (record.get(2) != null && record.get(2) instanceof BigDecimal)
					recordId = ((BigDecimal) record.get(2)).intValue();
				if (tableId > 0 && recordId > 0)
					addLog(recordId, null, TWO, fileName, tableId, recordId);
				else
					addLog (0, null, TWO, fileName);
			}
		}

		if (orphanFiles.size() > 0) {
			addLog("** Orphan Files **");
			for (String fileName : orphanFiles) {
				addLog (0, null, THREE, fileName);
			}
		}

		if (orphanRecords.size() > 0) {
			addLog("** Orphan Records **");
			for (List<Object> record : orphanRecords) {
				String originTableName = record.get(3).toString();
				int tableId  = 0;
				if (record.get(1) != null && record.get(1) instanceof BigDecimal)
					tableId = ((BigDecimal) record.get(1)).intValue();
				int recordId = 0;
				if (record.get(2) != null && record.get(2) instanceof BigDecimal)
					recordId = ((BigDecimal) record.get(2)).intValue();
				StringBuilder msg = new StringBuilder("Orphan record ")
						.append(originTableName)
						.append(" [ AD_Table_ID = ")
						.append(tableId)
						.append(", Record_ID = ")
						.append(recordId)
						.append(" ]");
				if (MAttachment.Table_Name.equals(originTableName)) {
					int attachId = DB.getSQLValueEx(get_TrxName(), "SELECT AD_Attachment_ID FROM AD_Attachment WHERE AD_Table_ID=? AND Record_ID=?", tableId, recordId);
					addLog(recordId, null, FOUR, msg.toString(), MAttachment.Table_ID, attachId);
				} else if (MArchive.Table_Name.equals(originTableName)) {
					int archiveId = DB.getSQLValueEx(get_TrxName(), "SELECT AD_Archive_ID FROM AD_Archive WHERE AD_Table_ID=? AND Record_ID=?", tableId, recordId);
					addLog(recordId, null, FOUR, msg.toString(), MArchive.Table_ID, archiveId);
				} else {
					addLog(recordId, null, FOUR, msg.toString());
				}
			}
		}

		return records.size() + " files checked, "
			+ missingFiles.size() + " files not found, "
			+ emptyFiles.size() + " empty files, "
			+ orphanFiles.size() + " orphan files, "
			+ orphanRecords.size() + " orphan records";
	}

	private static boolean existInArray(List<List<Object>> orphanRecords, List<Object> record) {
		for (List<Object> orphanRecord : orphanRecords) {
			int orphanTableId  = 0;
			if (orphanRecord.get(1) != null && orphanRecord.get(1) instanceof BigDecimal)
				orphanTableId = ((BigDecimal) orphanRecord.get(1)).intValue();
			int orphanRecordId = 0;
			if (orphanRecord.get(2) != null && orphanRecord.get(2) instanceof BigDecimal)
				orphanRecordId = ((BigDecimal) orphanRecord.get(2)).intValue();
			int tableId  = 0;
			if (record.get(1) != null && record.get(1) instanceof BigDecimal)
				tableId = ((BigDecimal) record.get(1)).intValue();
			int recordId = 0;
			if (record.get(2) != null && record.get(2) instanceof BigDecimal)
				recordId = ((BigDecimal) record.get(2)).intValue();
			if (tableId == orphanTableId && recordId == orphanRecordId)
				return true;
		}
		return false;
	}

}

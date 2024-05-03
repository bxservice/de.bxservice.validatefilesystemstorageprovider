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
import java.util.List;

import org.compiere.model.MProcessPara;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;

/**
 * SSV Exporter for DATEV
 */
public class CheckFiles extends SvrProcess{

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
		sql.append("    AS filename, ad_table_id, record_id ");
		sql.append("FROM ( ");
		sql.append("SELECT ");
		sql.append("	REPLACE( ");
		sql.append("	    UNNEST( ");
		sql.append("	      XPATH('//attachments/entry/@file', ");
		sql.append("	          CONVERT_FROM(a.binarydata, 'UTF-8')::xml ");
		sql.append("	           ))::varchar ");
		sql.append("	        , '%ATTACHMENT_FOLDER%', s.folder||'/') AS filename ");
		sql.append("    , a.ad_table_id, a.record_id ");
		sql.append("FROM ad_attachment a ");
		sql.append("  JOIN ad_storageprovider s ON (a.ad_storageprovider_id=s.ad_storageprovider_id AND s.METHOD='FileSystem') ");
		sql.append("WHERE a.title = 'xml' ");
		sql.append("  AND a.binarydata IS NOT NULL ");
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
		sql.append("    , a.ad_table_id, a.record_id ");
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
		sql.append("    , 0, 0 ");
		sql.append("FROM ad_image a ");
		sql.append("  JOIN ad_storageprovider s ON (a.ad_storageprovider_id=s.ad_storageprovider_id AND s.METHOD='FileSystem') ");
		sql.append("WHERE a.binarydata IS NOT null ");
		if (getAD_Client_ID() > 0)
			sql.append("  AND a.ad_client_id=? ");
		sql.append(") f ");
		sql.append("ORDER BY 1");

		List<List<Object>> filenames;
		if (getAD_Client_ID() > 0)
			filenames = DB.getSQLArrayObjectsEx(get_TrxName(), sql.toString(), getAD_Client_ID(), getAD_Client_ID(), getAD_Client_ID());
		else
			filenames = DB.getSQLArrayObjectsEx(get_TrxName(), sql.toString());
		int cnt = 0;
		int cntNotFound = 0;
		int cntEmpty = 0;
		for (List<Object> objs : filenames) {
			if (cnt % 1000 == 0)
				statusUpdate("Processing ... " + cnt + " / " + objs.size());
			String filename = objs.get(0).toString();
			int tableId  = ((BigDecimal) objs.get(1)).intValue();
			int recordId = ((BigDecimal) objs.get(2)).intValue();
			File file = new File(filename);
			if (! file.exists()) {
				if (tableId > 0 && recordId > 0)
					addLog(recordId, null, null, filename, tableId, recordId);
				else
					addLog(filename);
				cntNotFound++;
			} else if (file.length() == 0) {
				if (tableId > 0 && recordId > 0)
					addLog(recordId, null, null, "SIZE ZERO! -> " + filename, tableId, recordId);
				else
					addLog(filename);
				cntEmpty++;
			}
			cnt++;
		}

		return cnt + " files checked, " + cntNotFound + " files not found, " + cntEmpty + " empty files";
	}

}

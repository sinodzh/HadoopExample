package com.du.hbase.coprocessor;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

/**
 * @FileName : (CP_writelog.java)
 *
 * @description :协处理器写日志
 * @author : Frank.Du
 * @version : Version No.1
 * @create : 2016年12月8日 下午6:52:31
 * @modify : 2016年12月8日 下午6:52:31
 * @copyright :
 */
public class CP_writelog extends BaseRegionObserver {
	private static final Logger logger = Logger.getLogger(CP_writelog.class);

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, Durability durability) throws IOException {

		writeLog(put);

		super.prePut(e, put, edit, durability);
	}

	/**
	 * 写log
	 * 
	 * @param put
	 * @throws IOException
	 */
	private void writeLog(Put put) {
		try {
			logger.info("writehdfs : is begining");
			CellScanner cellScanner = put.cellScanner();

			StringBuilder sb = new StringBuilder();
			while (cellScanner.advance()) {
				Cell current = cellScanner.current();
				String fieldName = new String(CellUtil.cloneQualifier(current),
						"utf-8");
				String fieldValue = new String(CellUtil.cloneValue(current),
						"utf-8");
				String fieldRow = new String(CellUtil.cloneRow(current),
						"utf-8");

				String fieldFamilyCell = new String(
						CellUtil.cloneFamily(current), "utf-8");
				String info = "fieldName:" + fieldName + " fieldValue:"
						+ fieldValue + " fieldRow:" + fieldRow
						+ " fieldFamilyCell:" + fieldFamilyCell;

				sb.append(info);
			}

			logger.info("writehdfs : info:" + sb.toString());

		} catch (IOException e) {
			logger.error(e.getMessage());
		}

	}

}

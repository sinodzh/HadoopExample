package com.du.solr.demo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

/**
 * @FileName : (CloudSolrServerDemo.java)
 *
 * @description :
 * @author : Frank.Du
 * @version : Version No.1
 * @create : 2016年12月30日 上午9:48:59
 * @modify : 2016年12月30日 上午9:48:59
 * @copyright :
 */
public class CloudSolrServerDemo {

	SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	SimpleDateFormat sdfDay = new SimpleDateFormat("yyyy-MM-dd");

	private void insert() throws SolrServerException, IOException,
			ParseException {
		String zhHost = "master1/solr";

		CloudSolrServer cloudSolrServer = new CloudSolrServer(zhHost);

		cloudSolrServer.setDefaultCollection("date_demo");

		String id_1 = UUID.randomUUID().toString().replaceAll("-", "")
				.toUpperCase();
		String name_1 = "1点前+8";
		Date createDate_1 = sdfDate.parse("2016-12-30 00:11:12");
		String day_1 = sdfDay.format(createDate_1);

		String id_2 = UUID.randomUUID().toString().replaceAll("-", "")
				.toUpperCase();
		String name_2 = "1点后+8";
		Date createDate_2 = sdfDate.parse("2016-12-30 10:13:14");
		String day_2 = sdfDay.format(createDate_2);

		SolrInputDocument solrInputDocument1 = create(id_1, name_1, day_1,
				createDate_1);
		SolrInputDocument solrInputDocument2 = create(id_2, name_2, day_2,
				createDate_2);

		cloudSolrServer.add(solrInputDocument1);
		cloudSolrServer.add(solrInputDocument2);
		cloudSolrServer.commit();

		System.out.println("success");
	}

	private void query() throws SolrServerException {
		String zhHost = "master1/solr";

		CloudSolrServer cloudSolrServer = new CloudSolrServer(zhHost);

		cloudSolrServer.setDefaultCollection("date_demo");

		String queryStr = "*:*";

		SolrQuery sQuery = new SolrQuery();

		sQuery.setQuery(queryStr);

		QueryResponse query = cloudSolrServer.query(sQuery);
		SolrDocumentList results = query.getResults();
		for (SolrDocument solrDocument : results) {
			String id = solrDocument.getFieldValue("ID").toString();
			String name = solrDocument.getFieldValue("NAME").toString();
			String day = solrDocument.getFieldValue("CREATEDAY").toString();
			String date = solrDocument.getFieldValue("CREATEDATE").toString();
			String info = id + " ," + name + "  ," + day + " ," + date;
			System.out.println(info);
		}

	}

	private SolrInputDocument create(String id, String name, String day,
			Date createDate) {
		SolrInputDocument solrInputDocument = new SolrInputDocument();

		solrInputDocument.setField("ID", id);
		solrInputDocument.setField("NAME", name);
		solrInputDocument.setField("CREATEDAY", day);
		solrInputDocument.setField("CREATEDATE", createDate);

		return solrInputDocument;
	}

	public static void main(String[] args) throws SolrServerException,
			IOException, ParseException {
		CloudSolrServerDemo cloudSolrServerDemo = new CloudSolrServerDemo();
		cloudSolrServerDemo.insert();
		cloudSolrServerDemo.query();

	}
}

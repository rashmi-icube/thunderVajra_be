package org.owen.hr;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.owen.helper.DatabaseConnectionHelper;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;

public class HrDashboardHelper {

	public String getResponseCount(int questionId) {
		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getResponseCount(?)}")) {
			cstmt.setInt("queId", questionId);
			try (ResultSet res = cstmt.executeQuery()) {
				while (res.next()) {
					JSONObject json = new JSONObject();
					json.put("responseCount", res.getInt("response_count"));
					result.put(json);
				}
			} catch (JSONException e) {
				Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving sentiment score", e);
			}
		} catch (SQLException e1) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving sentiment score", e1);
		}
		return result.toString();
	}

	public String getWordCloudAssociation(int questionId) {
		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		RConnection rCon = dch.getRConn();
		try {
			rCon.assign("queId", new int[] { questionId });

			REXP wordCA = rCon.parseAndEval("try(eval(GetWords(queId)))");
			if (wordCA.inherits("try-error")) {
				org.apache.log4j.Logger.getLogger(HrDashboardHelper.class).error("Error: " + wordCA.asString());
				dch.releaseRcon();
				throw new Exception("Error: " + wordCA.asString());
			} else {
				org.apache.log4j.Logger.getLogger(HrDashboardHelper.class).debug("Retrieval of the employee smart list completed " + wordCA.asList());
			}

			RList r = wordCA.asList();

			REXPDouble frequency = (REXPDouble) r.get("frequency");
			double[] freqArray = frequency.asDoubles();

			REXPString words = (REXPString) r.get("words");
			String[] wordArray = words.asStrings();

			REXPString associations = (REXPString) r.get("association");
			String[] associationsArray = associations.asStrings();

			for (int i = 0; i < freqArray.length; i++) {
				JSONObject json = new JSONObject();
				json.put("word", wordArray[i]);
				json.put("frequency", freqArray[i]);
				json.put("association", associationsArray[i]);
				result.put(json);
			}
		} catch (Exception e) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving word cloud association", e);
		}
		dch.releaseRcon();
		return result.toString();

	}

	public String getSentimentDistribution(int questionId) {
		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		RConnection rCon = dch.getRConn();
		JSONObject avg = new JSONObject();

		try {
			rCon.assign("queId", new int[] { questionId });

			REXP sentimentDist = rCon.parseAndEval("try(eval(GetSentimentDistribution(queId)))");
			if (sentimentDist.inherits("try-error")) {
				org.apache.log4j.Logger.getLogger(HrDashboardHelper.class).error("Error: " + sentimentDist.asString());
				dch.releaseRcon();
				throw new Exception("Error: " + sentimentDist.asString());
			} else {
				org.apache.log4j.Logger.getLogger(HrDashboardHelper.class).debug(
						"Retrieval of the employee smart list completed " + sentimentDist.asList());
			}

			RList r = sentimentDist.asList();

			REXPString sentiments = (REXPString) r.get("sentiment");
			String[] sentimentArray = sentiments.asStrings();

			REXPInteger count = (REXPInteger) r.get("count");
			int[] countArray = count.asIntegers();

			int totalCount = 0;
			int productTotalCount = 0;

			for (int i = 0; i < sentimentArray.length; i++) {
				JSONObject json = new JSONObject();
				json.put("name", sentimentArray[i]);
				int[] dataArray = {countArray[i]};
				json.put("data", dataArray);
				totalCount += countArray[i];
				if (sentimentArray[i].equals("negative")) {
					productTotalCount += (1 * countArray[i]);
				} else if (sentimentArray[i].equals("neutral")) {
					productTotalCount += (3 * countArray[i]);
				} else if (sentimentArray[i].equals("positive")) {
					productTotalCount += (5 * countArray[i]);
				}
				result.put(json);
			}

			avg.put("average", productTotalCount / totalCount);
		} catch (Exception e) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving word cloud association", e);
		}
		dch.releaseRcon();
		return result.toString() + ";" + avg.toString();

	}
}

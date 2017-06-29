package org.owen.hr;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	public String getResponseCount() {
		Logger.getLogger(HrDashboardHelper.class).debug("Entering getResponseCount");
		JSONObject json = new JSONObject();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getResponseCount()}")) {
			try (ResultSet res = cstmt.executeQuery()) {
				while (res.next()) {
					json.put(String.valueOf(res.getInt("que_id")), res.getInt("response_count"));
				}
			} catch (JSONException e) {
				Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving sentiment score", e);
			}
		} catch (SQLException e1) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving sentiment score", e1);
		}
		return json.toString();
	}

	public String getWordCloudAssociation() {
		Logger.getLogger(HrDashboardHelper.class).debug("Entering getWordCloudAssociation");
		JSONObject result = new JSONObject();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		RConnection rCon = dch.getRConn();
		try {

			REXP wordCA = rCon.parseAndEval("try(eval(GetWords()))");
			if (wordCA.inherits("try-error")) {
				org.apache.log4j.Logger.getLogger(HrDashboardHelper.class).error("Error: " + wordCA.asString());
				dch.releaseRcon();
				throw new Exception("Error: " + wordCA.asString());
			} else {
				org.apache.log4j.Logger.getLogger(HrDashboardHelper.class).debug("Retrieval of word cloud association completed " + wordCA.asList());
			}

			RList r = wordCA.asList();

			REXPInteger questionIds = (REXPInteger) r.get("que_id");
			int[] qIdArray = questionIds.asIntegers();

			REXPDouble frequency = (REXPDouble) r.get("frequency");
			double[] freqArray = frequency.asDoubles();

			REXPString words = (REXPString) r.get("words");
			String[] wordArray = words.asStrings();

			REXPString associations = (REXPString) r.get("association");
			String[] associationsArray = associations.asStrings();

			REXPString sentiments = (REXPString) r.get("sentiment");
			String[] sentimentsArray = sentiments.asStrings();

			Map<Integer, List<Map<String, Object>>> resultMap = new HashMap<>();

			for (int i = 0; i < qIdArray.length; i++) {
				int qId = qIdArray[i];
				Map<String, Object> innerMap = new HashMap<>();
				innerMap.put("word", wordArray[i]);
				innerMap.put("frequency", freqArray[i]);
				innerMap.put("association", associationsArray[i]);
				innerMap.put("sentiment", sentimentsArray[i]);
				if (resultMap.containsKey(qId)) {
					resultMap.get(qId).add(innerMap);
				} else {
					resultMap.put(qId, new ArrayList<>());
					resultMap.get(qId).add(innerMap);
				}
			}

			for (int questionId : resultMap.keySet()) {
				List<Map<String, Object>> list = resultMap.get(questionId);
				JSONArray innerArray = new JSONArray();
				for (int i = 0; i < list.size(); i++) {
					Map<String, Object> innerMap = list.get(i);
					JSONObject innerObject = new JSONObject();
					for (String key : innerMap.keySet()) {
						innerObject.put(key, innerMap.get(key));
					}
					innerArray.put(innerObject);
				}
				result.put(String.valueOf(questionId), innerArray);
			}

		} catch (Exception e) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving word cloud association", e);
		}
		dch.releaseRcon();
		return result.toString();

	}

	public String getSentimentDistribution() {
		Logger.getLogger(HrDashboardHelper.class).debug("Entering getSentimentDistribution");
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		RConnection rCon = dch.getRConn();
		JSONObject avg = new JSONObject();
		JSONObject jObj = new JSONObject();
		try {
			REXP sentimentDist = rCon.parseAndEval("try(eval(GetSentimentDistribution()))");
			if (sentimentDist.inherits("try-error")) {
				org.apache.log4j.Logger.getLogger(HrDashboardHelper.class).error("Error: " + sentimentDist.asString());
				dch.releaseRcon();
				throw new Exception("Error: " + sentimentDist.asString());
			} else {
				org.apache.log4j.Logger.getLogger(HrDashboardHelper.class).debug("Fetched the sentiment distribution :::  " + sentimentDist.asList());
			}

			RList r = sentimentDist.asList();

			REXPInteger questionIds = (REXPInteger) r.get("que_id");
			int[] qIdArray = questionIds.asIntegers();

			REXPString sentiments = (REXPString) r.get("sentiment");
			String[] sentimentArray = sentiments.asStrings();

			REXPInteger count = (REXPInteger) r.get("count");
			int[] countArray = count.asIntegers();

			Map<Integer, Map<String, Integer>> resultMap = new HashMap<>();

			for (int i = 0; i < qIdArray.length; i++) {
				int qId = qIdArray[i];
				if (resultMap.containsKey(qId)) {
					Map<String, Integer> innerMap = resultMap.get(qId);
					innerMap.put(sentimentArray[i], countArray[i]);
					resultMap.put(qId, innerMap);
				} else {
					Map<String, Integer> innerMap = new HashMap<>();
					innerMap.put(sentimentArray[i], countArray[i]);
					resultMap.put(qId, innerMap);
				}
			}

			for (int questionId : resultMap.keySet()) {
				int totalCount = 0;
				int productTotalCount = 0;
				Map<String, Integer> sourceMap = resultMap.get(questionId);
				JSONArray innerArray = new JSONArray();
				for (String key : sourceMap.keySet()) {
					JSONObject innerObj = new JSONObject();
					innerObj.put("name", key);
					// int[] dataArray = { sourceMap.get(key) };
					String color = "";
					if (key.equalsIgnoreCase("positive")) {
						color = "#64DD17";
					} else if (key.equalsIgnoreCase("negative")) {
						color = "#DD2C00";
					} else {
						color = "#FFD600";
					}

					String[] dataArray = new String[2];
					dataArray[0] = "y : " + sourceMap.get(key);
					dataArray[1] = "color : " + color;

					JSONObject dataObj = new JSONObject();
					dataObj.put("y", sourceMap.get(key));
					dataObj.put("color", color);
					JSONArray dataArr = new JSONArray();
					dataArr.put(dataObj);
					innerObj.put("data", dataArr);

					// innerObj.put("data", dataArray);
					totalCount += sourceMap.get(key);
					if (key.equals("negative")) {
						productTotalCount += (1 * sourceMap.get(key));
					} else if (key.equals("neutral")) {
						productTotalCount += (3 * sourceMap.get(key));
					} else if (key.equals("positive")) {
						productTotalCount += (5 * sourceMap.get(key));
					}
					innerArray.put(innerObj);
				}
				jObj.put(String.valueOf(questionId), innerArray);
				avg.put(String.valueOf(questionId), productTotalCount / totalCount);
			}

		} catch (Exception e) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving word cloud association", e);
		}
		dch.releaseRcon();
		return jObj.toString() + ";" + avg.toString();

	}
}

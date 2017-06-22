package org.owen.hr;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.owen.helper.DatabaseConnectionHelper;

public class HrDashboardHelper {

	

	public String getRelIndexValue(int companyId, int functionId, int positionId, int locationId) {
		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall(
				"{call getRelIndexValue(?,?,?)}")) {
			cstmt.setInt("fun", functionId);
			cstmt.setInt("pos", positionId);
			cstmt.setInt("loc", locationId);
			try (ResultSet res = cstmt.executeQuery()) {
				while (res.next()) {
					JSONObject json = new JSONObject();
					json.put("relId", res.getInt("rel_id"));
					json.put("indexValue", res.getDouble("metric_value"));
					json.put("explanation", res.getString("explanation"));
					json.put("action", res.getString("action"));
					json.put("responseCount", res.getInt("response_count"));
					result.put(json);
				}
			} catch (JSONException e) {
				Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving index value", e);

			}
		} catch (SQLException e1) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving index value", e1);
		}
		return result.toString();
	}

	public String getRelKeyPeople(int companyId, int functionId, int positionId, int locationId) {
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		JSONArray result = new JSONArray();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall(
				"{call getRelKeyPeople(?,?,?)}")) {
			cstmt.setInt("fun", functionId);
			cstmt.setInt("pos", positionId);
			cstmt.setInt("loc", locationId);
			Map<Double, String> rel1 = new TreeMap<>();
			Map<Double, String> rel2 = new TreeMap<>();
			Map<Double, String> rel3 = new TreeMap<>();
			Map<Double, String> rel4 = new TreeMap<>();
			try (ResultSet res = cstmt.executeQuery()) {
				while (res.next()) {
					if (res.getInt("rel_id") == 1) {
						rel1.put(res.getDouble("emp_rank"), res.getString("first_name") + " " + res.getString("last_name"));
					} else if (res.getInt("rel_id") == 2) {
						rel2.put(res.getDouble("emp_rank"), res.getString("first_name") + " " + res.getString("last_name"));
					} else if (res.getInt("rel_id") == 3) {
						rel3.put(res.getDouble("emp_rank"), res.getString("first_name") + " " + res.getString("last_name"));
					} else if (res.getInt("rel_id") == 4) {
						rel4.put(res.getDouble("emp_rank"), res.getString("first_name") + " " + res.getString("last_name"));
					}

					// JSONObject json = new JSONObject();
					// json.put("relId", res.getInt("rel_id"));
					// json.put("empId", res.getInt("emp_id"));
					// json.put("firstName", res.getString("first_name"));
					// json.put("lastName", res.getString("last_name"));
					// json.put("rank", res.getInt("emp_rank"));
					// result.put(json);
				}

				JSONObject json1 = new JSONObject();
				json1.put("relId", 1);
				json1.put("keyPeople", rel1.values());
				result.put(json1);

				JSONObject json2 = new JSONObject();
				json2.put("relId", 2);
				json2.put("keyPeople", rel2.values());
				result.put(json2);

				JSONObject json3 = new JSONObject();
				json3.put("relId", 3);
				json3.put("keyPeople", rel3.values());
				result.put(json3);

				JSONObject json4 = new JSONObject();
				json4.put("relId", 4);
				json4.put("keyPeople", rel4.values());
				result.put(json4);

			}
		} catch (JSONException | SQLException e) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving key people", e);
		}
		return result.toString();
	}

	public String getWordCloud(int companyId, int functionId, int positionId, int locationId) {
		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall(
				"{call getWordCloud(?,?,?)}")) {
			cstmt.setInt("fun", functionId);
			cstmt.setInt("pos", positionId);
			cstmt.setInt("loc", locationId);

			try (ResultSet res = cstmt.executeQuery()) {
				while (res.next()) {
					JSONObject json = new JSONObject();
					json.put("relId", res.getInt("rel_id"));
					json.put("word", res.getString("word"));
					json.put("weight", res.getInt("weight"));
					result.put(json);
				}
			}
		} catch (JSONException | SQLException e) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving word cloud", e);
		}
		return result.toString();
	}

	public String getSentimentScore(int companyId, int functionId, int positionId, int locationId) {
		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall(
				"{call getSentimentScore(?,?,?)}")) {
			cstmt.setInt("fun", functionId);
			cstmt.setInt("pos", positionId);
			cstmt.setInt("loc", locationId);

			try (ResultSet res = cstmt.executeQuery()) {
				while (res.next()) {
					JSONObject json = new JSONObject();
					json.put("relId", res.getInt("rel_id"));
					json.put("metricValue", res.getInt("metric_value"));
					json.put("explanation", res.getString("explanation"));
					json.put("action", res.getString("action"));
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

	public String getSelfPerception(int companyId, int functionId, int positionId, int locationId) {

		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall(
				"{call getSelfPerception(?,?,?)}")) {
			cstmt.setInt("fun", functionId);
			cstmt.setInt("pos", positionId);
			cstmt.setInt("loc", locationId);

			try (ResultSet res = cstmt.executeQuery()) {
				while (res.next()) {
					JSONObject teamJson = new JSONObject();
					teamJson.put("relId", res.getInt("rel_id"));
					teamJson.put("relName", res.getString("rel_name"));
					teamJson.put("explanation", res.getString("explanation"));
					teamJson.put("action", res.getString("action"));
					teamJson.put("responseCount", res.getInt("response_count"));
					teamJson.put("level", "team");

					float teamTotalResponse = res.getInt("strongly_disagree") + res.getInt("disagree") + res.getInt("neutral") + res.getInt("agree")
							+ res.getInt("strongly_agree");

					teamJson.put("stronglyDisagree", Math.round((((float) res.getInt("strongly_disagree") / teamTotalResponse) * 100)));
					teamJson.put("disagree", Math.round((((float) res.getInt("disagree") / teamTotalResponse) * 100)));
					teamJson.put("neutral", Math.round((((float) res.getInt("neutral") / teamTotalResponse) * 100)));
					teamJson.put("agree", Math.round((((float) res.getInt("agree") / teamTotalResponse) * 100)));
					teamJson.put("stronglyAgree", Math.round((((float) res.getInt("strongly_agree") / teamTotalResponse) * 100)));
					result.put(teamJson);

					float orgTotalResponse = res.getInt("strongly_disagree_o") + res.getInt("disagree_o") + res.getInt("neutral_o")
							+ res.getInt("agree_o") + res.getInt("strongly_agree_o");

					JSONObject orgJson = new JSONObject();
					orgJson.put("relId", res.getInt("rel_id"));
					orgJson.put("relName", res.getString("rel_name"));
					orgJson.put("explanation", res.getString("explanation"));
					orgJson.put("action", res.getString("action"));
					orgJson.put("responseCount", res.getInt("response_count"));
					orgJson.put("level", "org");
					orgJson.put("stronglyDisagree", Math.round((((float) res.getInt("strongly_disagree_o") / orgTotalResponse) * 100)));
					orgJson.put("disagree", Math.round((((float) res.getInt("disagree_o") / orgTotalResponse) * 100)));
					orgJson.put("neutral", Math.round((((float) res.getInt("neutral_o") / orgTotalResponse) * 100)));
					orgJson.put("agree", Math.round((((float) res.getInt("agree_o") / orgTotalResponse) * 100)));
					orgJson.put("stronglyAgree", Math.round((((float) res.getInt("strongly_agree_o") / orgTotalResponse) * 100)));
					result.put(orgJson);
				}
			}
		} catch (JSONException | SQLException e) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving self perception", e);
		}
		return result.toString();

	}

	public String getExploreData(int companyId) {

		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getExploreData()}");
				ResultSet res = cstmt.executeQuery()) {
			while (res.next()) {
				JSONObject json = new JSONObject();
				json.put("indexValue", res.getInt("metric_value"));
				json.put("function", res.getString("Function"));
				json.put("position", res.getString("Position"));
				json.put("location", res.getString("Location"));
				json.put("relName", res.getString("rel_name"));
				result.put(json);
			}

		} catch (JSONException | SQLException e) {
			Logger.getLogger(HrDashboardHelper.class).error("Error while retrieving self perception", e);
		}
		return result.toString();

	}
}

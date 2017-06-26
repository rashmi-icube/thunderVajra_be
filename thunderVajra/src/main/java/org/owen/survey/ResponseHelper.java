package org.owen.survey;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.owen.helper.DatabaseConnectionHelper;

public class ResponseHelper {

	/**
	 * Save all responses for ME/WE/MOOD question using the same function
	 * @param responseList - answer objects
	 * @return true/false - if the response is saved or not
	 */
	public boolean saveAllResponses(List<Response> responseList) {
		boolean allResponsesSaved = true;

		if (!responseList.isEmpty()) {
			Logger.getLogger(ResponseHelper.class).debug("Entering saveAllResponses");

			for (int i = 0; i < responseList.size(); i++) {
				Response respObj = responseList.get(i);
				Logger.getLogger(ResponseHelper.class).debug("Entering saveAllResponses (TEXT) for question ID" + respObj.getQuestionId());
				boolean flag = saveTextResponse(respObj.getQuestionId(), respObj.getResponseString());
				allResponsesSaved = (allResponsesSaved || flag);
			}
		}
		return allResponsesSaved;
	}

	public boolean saveTextResponse(int questionId, String responseText) {
		boolean responseSaved = false;
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call saveResponse(?,?)}")) {
			cstmt.setInt("queId", questionId);
			cstmt.setString("resp", responseText);
			Logger.getLogger(ResponseHelper.class).debug("SQL statement for question : " + questionId + " : " + cstmt.toString());
			try (ResultSet rs = cstmt.executeQuery()) {
				if (rs.next()) {
					Logger.getLogger(ResponseHelper.class).debug("RS statement for question : " + questionId + " : " + rs.toString());
					responseSaved = true;
					Logger.getLogger(ResponseHelper.class).debug("Successfully saved the response for : " + questionId);
				}
			}

		} catch (Exception e) {
			Logger.getLogger(ResponseHelper.class).error("Exception while saving the response for question : " + questionId, e);
		}

		return responseSaved;
	}

	
}

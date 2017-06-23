package org.owen.survey;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.owen.helper.DatabaseConnectionHelper;

public class Question {

	private int questionId;
	private String questionText;

	public String getQuestionText() {
		return questionText;
	}

	public void setQuestionText(String questionText) {
		this.questionText = questionText;
	}

	public int getQuestionId() {
		return questionId;
	}

	public void setQuestionId(int questionId) {
		this.questionId = questionId;
	}

	public List<Question> getQuestionList() {
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		List<Question> questionList = new ArrayList<>();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getQuestion()}")) {
			try (ResultSet rs = cstmt.executeQuery()) {
				while (rs.next()) {
					Question q = new Question();
					q.setQuestionId(rs.getInt("que_id"));
					q.setQuestionText(rs.getString("question"));
					Logger.getLogger(Question.class).debug("Question for employee : " + q.getQuestionId() + " - " + q.getQuestionText());

					questionList.add(q);
				}
			}

		} catch (SQLException e) {
			Logger.getLogger(Question.class).error("Exception while retrieving the questionList", e);
		}

		return questionList;
	}

}

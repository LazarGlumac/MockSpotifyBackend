package com.csc301.profilemicroservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import org.json.*;

import org.springframework.stereotype.Repository;
import org.neo4j.driver.v1.Transaction;

@Repository
public class ProfileDriverImpl implements ProfileDriver {

	Driver driver = ProfileMicroserviceApplication.driver;

	public static void InitProfileDb() {
		String queryStr;

		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				queryStr = "CREATE CONSTRAINT ON (nProfile:profile) ASSERT exists(nProfile.userName)";
				trans.run(queryStr);

				queryStr = "CREATE CONSTRAINT ON (nProfile:profile) ASSERT exists(nProfile.password)";
				trans.run(queryStr);

				queryStr = "CREATE CONSTRAINT ON (nProfile:profile) ASSERT nProfile.userName IS UNIQUE";
				trans.run(queryStr);

				trans.success();
			}
			session.close();
		}
	}

	@Override
	public DbQueryStatus createUserProfile(String userName, String fullName, String password) {

		DbQueryStatus queryStatus;

		if (userName == null || fullName == null || password == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					String queryStr = String.format("CREATE (p:profile {userName: \"%1$s\", password: \"%2$s\"})",
							userName, password);
					trans.run(queryStr);

					queryStr = String.format("CREATE (p:playlist {plName: \"%1$s-favorites\"})", userName);
					trans.run(queryStr);

					queryStr = String.format(
							"MATCH (p:profile), (pl:playlist) WHERE p.userName = \"%1$s\" AND pl.plName = \"%1$s-favorites\" CREATE (p)-[:created]->(pl)",
							userName);
					trans.run(queryStr);

					trans.success();

				}
				session.close();
			}

			queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
		}

		return queryStatus;
	}

	@Override
	public DbQueryStatus followFriend(String userName, String frndUserName) {
		DbQueryStatus queryStatus;

		if (userName == null || frndUserName == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					String queryStr = String.format(
							"MATCH (p1:profile), (p2:profile) WHERE p1.userName = \"%1$s\" AND p2.userName = \"%2$s\" MERGE (p1)-[:follows]->(p2)",
							userName, frndUserName);
					trans.run(queryStr);

					trans.success();

				}
				session.close();
			}

			queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
		}

		return queryStatus;
	}

	@Override
	public DbQueryStatus unfollowFriend(String userName, String frndUserName) {
		DbQueryStatus queryStatus;

		if (userName == null || frndUserName == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					String queryStr = String.format(
							"MATCH (:profile {userName: \"%1$s\"})-[f:follows]->(:profile {userName: \"%2$s\"}) DELETE f",
							userName, frndUserName);
					trans.run(queryStr);

					trans.success();

				}
				session.close();
			}

			queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
		}

		return queryStatus;
	}

	@Override
	public DbQueryStatus getAllSongFriendsLike(String userName) {

		DbQueryStatus queryStatus;

		if (userName == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			JSONObject allSongsFriendsLike = new JSONObject();
			
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					String queryStr = String.format(
							"MATCH (p:profile)-[:follows]->(f:profile) WHERE p.userName = \"%1$s\" RETURN f.userName",
							userName);

					StatementResult result = trans.run(queryStr);
					
					List<String>friends = new ArrayList<String>();
					List<String>songs = new ArrayList<String>();
					
					while (result.hasNext()) {
						friends.add(result.next().get(0).toString());
					}
					
					
					for (String friendName : friends) {
						String playlistName = (friendName + "-favorites").replaceAll("\"", "");
						queryStr = String.format(
								"MATCH (p:profile)-[:created]->(pl:playlist) WHERE p.userName = \"%1$s\" AND pl.plName = \"%2$s\" MATCH (pl:playlist)-[:includes]->(s:song) RETURN s.songId", 
								friendName, playlistName).replaceAll("\"\"", "\"");
						
						result = trans.run(queryStr);
						
						while (result.hasNext()) {
							songs.add(result.next().get(0).toString().replaceAll("\"",""));
						}
						
						allSongsFriendsLike.put(friendName.replaceAll("\"",""), songs);
						songs.clear();

					}

					trans.success();

				}
				session.close();
				
			}
			
			queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
			queryStatus.setData(allSongsFriendsLike.toMap());
		}

		return queryStatus;
	}
}

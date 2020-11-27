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
			boolean alreadyExists = false;
			
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					
					String queryStr = "MATCH (p:profile) WHERE p.userName = $username return p";
					StatementResult result = trans.run(queryStr, params);
					
					if (!result.hasNext()) {
						params.put("fullname", fullName);
						params.put("password", password);
						params.put("playlistName", userName+"-favorites");
						
						queryStr = "CREATE (p:profile {userName: $username, fullName: $fullname, password: $password})";
						trans.run(queryStr, params);

						queryStr = "CREATE (p:playlist {plName: $playlistName})";
						trans.run(queryStr, params);

						queryStr = "MATCH (p:profile), (pl:playlist) WHERE p.userName = $username AND pl.plName = $playlistName CREATE (p)-[:created]->(pl)";
						trans.run(queryStr, params);
					} else {
						alreadyExists = true;
					}

					trans.success();

				}
				session.close();
			}
			
			if (alreadyExists) {
				queryStatus = new DbQueryStatus("PROFILE USERNAME ALREADY EXISTS", DbQueryExecResult.QUERY_ERROR_GENERIC);
			} else {
				queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
			}
			

			
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
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					params.put("friendUsername", frndUserName);
					
					String queryStr = "MATCH (p1:profile), (p2:profile) WHERE p1.userName = $username AND p2.userName = $friendUsername MERGE (p1)-[:follows]->(p2)";
					trans.run(queryStr, params);

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
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					params.put("friendUsername", frndUserName);
					
					String queryStr = "MATCH (:profile {userName: $username})-[f:follows]->(:profile {userName: $friendUsername}) DELETE f";
					trans.run(queryStr, params);

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
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					
					String queryStr = "MATCH (p:profile)-[:follows]->(f:profile) WHERE p.userName = $username RETURN f.userName";
					StatementResult result = trans.run(queryStr, params);
					
					List<String>friends = new ArrayList<String>();
					List<String>songs = new ArrayList<String>();
					
					while (result.hasNext()) {
						friends.add(result.next().get(0).toString());
					}
					
					
					for (String friendName : friends) {
						params.put("friendName", friendName.replaceAll("\"", ""));
						params.put("friendPlaylist", (friendName + "-favorites").replaceAll("\"", ""));
						
						queryStr = "MATCH (p:profile)-[:created]->(pl:playlist) WHERE p.userName = $friendName AND pl.plName = $friendPlaylist MATCH (pl:playlist)-[:includes]->(s:song) RETURN s.songId";
						
						result = trans.run(queryStr, params);
						
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

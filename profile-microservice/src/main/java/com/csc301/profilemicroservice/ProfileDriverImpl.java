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

		boolean goodConnection = true;

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
						params.put("playlistName", userName + "-favorites");

						queryStr = "CREATE (p:profile {userName: $username, fullName: $fullname, password: $password})";
						trans.run(queryStr, params);

						queryStr = "MERGE (p:playlist {plName: $playlistName})";
						trans.run(queryStr, params);

						queryStr = "MATCH (p:profile), (pl:playlist) WHERE p.userName = $username AND pl.plName = $playlistName CREATE (p)-[:created]->(pl)";
						trans.run(queryStr, params);
					} else {
						alreadyExists = true;
					}

					trans.success();

				} catch (Exception e) {
					goodConnection = false;
				}
				session.close();
			} catch (Exception e) {
				goodConnection = false;
			}

			if (goodConnection) {
				if (alreadyExists) {
					queryStatus = new DbQueryStatus("PROFILE USERNAME ALREADY EXISTS",
							DbQueryExecResult.QUERY_ERROR_GENERIC);
				} else {
					queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
				}
			} else {
				queryStatus = new DbQueryStatus("FAILED TO CONNECT TO NEO4J", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}

		}

		return queryStatus;
	}

	@Override
	public DbQueryStatus followFriend(String userName, String frndUserName) {
		DbQueryStatus queryStatus;

		boolean goodConnection = true;
		boolean alreadyFollows = true;

		if (userName == null || frndUserName == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					params.put("friendUsername", frndUserName);

					String queryStr = "RETURN EXISTS ((:profile {userName: $username})-[:follows]->(:profile {userName: $friendUsername}))";
					StatementResult result  = trans.run(queryStr, params);
										
					if (result.next().get(0).toString().equals("FALSE")) {						
						queryStr = "MATCH (p1:profile), (p2:profile) WHERE p1.userName = $username AND p2.userName = $friendUsername MERGE (p1)-[:follows]->(p2)";
						trans.run(queryStr, params);
						alreadyFollows = false;
					}

					trans.success();

				} catch (Exception e) {
					goodConnection = false;
				}
				session.close();
			} catch (Exception e) {
				goodConnection = false;
			}

			if (goodConnection) {
				if (!alreadyFollows) {
					queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
				} else {
					queryStatus = new DbQueryStatus("ALREADY FOLLOWING", DbQueryExecResult.QUERY_ERROR_GENERIC);
				}
				
			} else {
				queryStatus = new DbQueryStatus("FAILED TO CONNECT TO NEO4J", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
		}

		return queryStatus;
	}

	@Override
	public DbQueryStatus unfollowFriend(String userName, String frndUserName) {
		DbQueryStatus queryStatus;

		boolean goodConnection = true;
		boolean alreadyFollows = false;

		if (userName == null || frndUserName == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					params.put("friendUsername", frndUserName);

					String queryStr = "RETURN EXISTS ((:profile {userName: $username})-[:follows]->(:profile {userName: $friendUsername}))";
					StatementResult result  = trans.run(queryStr, params);
					
					if (result.next().get(0).toString().equals("TRUE")) {
						queryStr = "MATCH (:profile {userName: $username})-[f:follows]->(:profile {userName: $friendUsername}) DELETE f";
						trans.run(queryStr, params);
						alreadyFollows = true;
					}

					trans.success();

				} catch (Exception e) {
					goodConnection = false;
				}
				session.close();
			} catch (Exception e) {
				goodConnection = false;
			}

			if (goodConnection) {
				if (alreadyFollows) {
					queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
				} else {
					queryStatus = new DbQueryStatus("NOT EVEN FOLLOWING USER", DbQueryExecResult.QUERY_ERROR_GENERIC);
				}
				
			} else {
				queryStatus = new DbQueryStatus("FAILED TO CONNECT TO NEO4J", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
		}

		return queryStatus;
	}

	@Override
	public DbQueryStatus getAllSongFriendsLike(String userName) {

		DbQueryStatus queryStatus;

		boolean goodConnection = true;

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

					List<String> friends = new ArrayList<String>();
					List<String> songs = new ArrayList<String>();

					while (result.hasNext()) {
						friends.add(result.next().get(0).toString());
					}

					for (String friendName : friends) {
						params.put("friendName", friendName.replaceAll("\"", ""));
						params.put("friendPlaylist", (friendName + "-favorites").replaceAll("\"", ""));

						queryStr = "MATCH (p:profile)-[:created]->(pl:playlist) WHERE p.userName = $friendName AND pl.plName = $friendPlaylist MATCH (pl:playlist)-[:includes]->(s:song) RETURN s.songId";

						result = trans.run(queryStr, params);

						while (result.hasNext()) {
							songs.add(result.next().get(0).toString().replaceAll("\"", ""));
						}

						allSongsFriendsLike.put(friendName.replaceAll("\"", ""), songs);
						songs.clear();

					}

					trans.success();

				} catch (Exception e) {
					goodConnection = false;
				}
				session.close();

			} catch (Exception e) {
				goodConnection = false;
			}

			if (goodConnection) {
				queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
				queryStatus.setData(allSongsFriendsLike.toMap());
			} else {
				queryStatus = new DbQueryStatus("FAILED TO CONNECT TO NEO4J", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}

		}

		return queryStatus;
	}
}

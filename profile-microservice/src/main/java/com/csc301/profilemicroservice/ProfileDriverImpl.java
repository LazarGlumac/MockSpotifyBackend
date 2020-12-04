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
		
		// Keeps track of whether the connection to Neo4j is successful
		boolean goodConnection = true;
		
		// Checks if any of the parameters are empty
		if (userName.isEmpty() || fullName.isEmpty() || password.isEmpty()) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			// Keeps track of whether a user with the given username already exists in Neo4j
			boolean alreadyExists = false;

			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					
					// Query to check if a user with the given username exists in Neo4j
					String queryStr = "MATCH (p:profile) WHERE p.userName = $username return p";
					StatementResult result = trans.run(queryStr, params);
					
					// Checks if a user already exists in Neo4j with the given username
					if (!result.hasNext()) {
						params.put("fullname", fullName);
						params.put("password", password);
						params.put("playlistName", userName + "-favorites");
						
						// Query to create a profile node in Neo4j with the given user information
						queryStr = "CREATE (p:profile {userName: $username, fullName: $fullname, password: $password})";
						trans.run(queryStr, params);
						
						// Query to create a playlist node in Neo4j for the new user
						queryStr = "MERGE (p:playlist {plName: $playlistName})";
						trans.run(queryStr, params);

						// Query to create a relationship between the user and their playlist
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

		// Keeps track of whether the connection to Neo4j is successful
		boolean goodConnection = true;
		
		// Keeps track of the given user already follows the friend
		boolean alreadyFollows = true;

		// Checks if the parameters are empty
		if (userName.isEmpty() || frndUserName.isEmpty()) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			// Checks if any of the given users exist in Neo4j
			if (!userExists(userName) || !userExists(frndUserName)) {
				return new DbQueryStatus("ONE OR MORE USERS NON-EXISTENT", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
			
			// Checking if the given usernames are the same, which would mean the user is trying to follow themselves
			if (userName.equals(frndUserName)) {
				return new DbQueryStatus("CAN'T ADD YOURSELF", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}

			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					params.put("friendUsername", frndUserName);
					
					// Query to check if the given user already follows the friend
					String queryStr = "RETURN EXISTS ((:profile {userName: $username})-[:follows]->(:profile {userName: $friendUsername}))";
					StatementResult result = trans.run(queryStr, params);
					
					// Checks if the given user already follows the friend
					if (result.next().get(0).toString().equals("FALSE")) {
						
						// Query to check create the relationship to have the given user follow the friend
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
		
		// Keeps track of whether the connection to Neo4j is successful 
		boolean goodConnection = true;
		
		// Keeps track of whether the given user is already following the friend
		boolean alreadyFollows = false;

		// Checks if any of the parameters are empty
		if (userName.isEmpty() || frndUserName.isEmpty()) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			// Checks if any of the given users exist in Neo4j
			if (!userExists(userName) || !userExists(frndUserName)) {
				return new DbQueryStatus("ONE OR MORE USERS NON-EXISTENT", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}

			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					params.put("friendUsername", frndUserName);
					
					// Query to check if the user already follows the friend
					String queryStr = "RETURN EXISTS ((:profile {userName: $username})-[:follows]->(:profile {userName: $friendUsername}))";
					StatementResult result = trans.run(queryStr, params);
					
					// Checks if the user already follows the friend
					if (result.next().get(0).toString().equals("TRUE")) {
						
						// Query to remove the user's follow relationship with the friend
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
		
		// Keeps track of whether the connection to Neo4j is successful
		boolean goodConnection = true;
		
		// Checks if the parameter is empty
		if (userName.isEmpty()) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			// Checks if a user with the given username exists in Neo4j
			if (!userExists(userName)) {
				return new DbQueryStatus("USER NON-EXISTENT", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
			
			/* Stores the liked songs of the given user's friends with key-value pairs, where the keys are the
		 	usernames of friends, and the values are lists containing the liked songs for each friend*/
			JSONObject allSongsFriendsLike = new JSONObject();

			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("username", userName);
					
					
					// Query to find all the given user's friends
					String queryStr = "MATCH (p:profile)-[:follows]->(f:profile) WHERE p.userName = $username RETURN f.userName";
					StatementResult result = trans.run(queryStr, params);
					
					// Stores the names of the user's friends
					List<String> friends = new ArrayList<String>();
					
					// A temporary list to store the current friend's liked songs
					List<String> songs = new ArrayList<String>();
					
					// Filling the friends list with the user's friends
					while (result.hasNext()) {
						friends.add(result.next().get(0).toString());
					}
					
					// Filling allSongsFriendsLike with the appropriate key-value pairing
					for (String friendName : friends) {
						params.put("friendName", friendName.replaceAll("\"", ""));
						params.put("friendPlaylist", (friendName + "-favorites").replaceAll("\"", ""));
						
						// Query to find the liked songs of each friend
						queryStr = "MATCH (p:profile)-[:created]->(pl:playlist) WHERE p.userName = $friendName AND pl.plName = $friendPlaylist MATCH (pl:playlist)-[:includes]->(s:song) RETURN s.songId";

						result = trans.run(queryStr, params);
						
						// Filling the temporary song array with the current friend's songs
						while (result.hasNext()) {
							songs.add(result.next().get(0).toString().replaceAll("\"", ""));
						}
						
						// Adding a friend-songs, key-value pair to allSongsFriendsLike
						allSongsFriendsLike.put(friendName.replaceAll("\"", ""), songs);
						
						// Clearing the temporary song list to be used for the next friend
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

	/**********************
	 *  HELPER FUNCTIONS  *
	 *********************/
	
	/**
	 * Checks if a user exists in the Neo4j database
	 * 
	 * @param username the userName property of the profile node in the Neo4j database
	 * @return true, if the user exists, false otherwise
	 */
	public boolean userExists(String username) {
		boolean exists = false;

		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("username", username);

				String queryStr = "MATCH (p:profile) WHERE p.userName = $username return p";
				StatementResult result = trans.run(queryStr, params);

				if (result.hasNext()) {
					exists = true;
				}

				trans.success();

			} catch (Exception e) {
			}
			session.close();
		} catch (Exception e) {
		}

		return exists;
	}
}

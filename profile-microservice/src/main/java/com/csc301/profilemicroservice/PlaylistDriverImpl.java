package com.csc301.profilemicroservice;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.springframework.stereotype.Repository;
import org.neo4j.driver.v1.Transaction;

@Repository
public class PlaylistDriverImpl implements PlaylistDriver {

	Driver driver = ProfileMicroserviceApplication.driver;

	public static void InitPlaylistDb() {
		String queryStr;

		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				queryStr = "CREATE CONSTRAINT ON (nPlaylist:playlist) ASSERT exists(nPlaylist.plName)";
				trans.run(queryStr);

				queryStr = "CREATE CONSTRAINT ON (nSong:song) ASSERT exists(nSong.songId)";
				trans.run(queryStr);

				trans.success();
			}
			session.close();
		}
	}

	
	/**
	 * Adds a song node into the Neo4j database with the given songId
	 * 
	 * @param songId the songId property of the song node in the Neo4j database
	 * @return the DbQueryStatus of the operation performed (OK for success, non OK o/w)
	 */
	public DbQueryStatus addSong(String songId) {

		DbQueryStatus queryStatus;

		// Keeps track of whether the connection to Neo4j is successful
		boolean goodConnection = true;

		// Checking if the parameter is empty
		if (songId.isEmpty()) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {

			// Checking if a song node exists in Neo4j with the given songId
			if (songExists(songId)) {
				return new DbQueryStatus("SONG DNE", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}

			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("songId", songId);

					// Query to create a song node in Neo4j with the given songId
					String queryStr = "MERGE (s:song {songId: $songId})";
					trans.run(queryStr, params);

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
			} else {
				queryStatus = new DbQueryStatus("FAILED TO CONNECT TO NEO4J", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}

		}

		return queryStatus;
	}

	/**
	 * Creates a direct relationship from the given user's playlist to the song labelled, "includes", in the Neo4j database
	 * 
	 * @param songId the songId property of the song node in the Neo4j database
	 * @param userName the userName property of the profile node in the Neo4j database
	 * @return the DbQueryStatus of the operation performed (OK for success, non OK o/w)
	 */
	@Override
	public DbQueryStatus likeSong(String userName, String songId) {

		DbQueryStatus queryStatus;

		// Keeps track of whether the connection to Neo4j is successful
		boolean goodConnection = true;

		// Keeps track of whether the song has already been liked by the user
		boolean alreadyLiked = false;

		// Checking if any of the parameters are empty
		if (userName.isEmpty() || songId.isEmpty()) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {

			// Checking if the song or user exists in Neo4j
			if (!songExists(songId) || !userExists(userName)) {
				return new DbQueryStatus("SONG OR USER DNE", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}

			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("songId", songId);
					params.put("playlistName", userName + "-favorites");

					// Query to check if the user has already liked the song
					String queryStr = "RETURN EXISTS ((:playlist {plName: $playlistName})-[:includes]->(:song {songId: $songId}))";
					StatementResult result = trans.run(queryStr, params);
					
					// Checking if the user has already liked the song
					if (result.next().get(0).toString().equals("FALSE")) {
						
						// Making the relationship between the user's playlist and song 
						queryStr = "MATCH (pl:playlist), (s:song) WHERE pl.plName = $playlistName AND s.songId = $songId MERGE (pl)-[:includes]->(s)";
						trans.run(queryStr, params);
					} else {
						alreadyLiked = true;
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
				if (!alreadyLiked) {
					queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
				} else {
					queryStatus = new DbQueryStatus("ALREADY LIKED", DbQueryExecResult.QUERY_OK);
				}

			} else {
				queryStatus = new DbQueryStatus("FAILED TO CONNECT TO NEO4J", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
		}

		return queryStatus;
	}

	/**
	 * Removes a direct relationship from the given user's playlist to the song labelled, "includes", in the Neo4j database
	 * 
	 * @param songId the songId property of the song node in the Neo4j database
	 * @param userName the userName property of the profile node in the Neo4j database
	 * @return the DbQueryStatus of the operation performed (OK for success, non OK o/w)
	 */
	@Override
	public DbQueryStatus unlikeSong(String userName, String songId) {

		DbQueryStatus queryStatus;

		// Keeps track of whether the connection to Neo4j is successful
		boolean goodConnection = true;
		
		// Keeps track of whether the song has been liked by the user
		boolean hasBeenLiked = false;
		
		// Checks if any of the parameters are empty
		if (userName.isEmpty() || songId.isEmpty()) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			// Checking if the given songId or user exists in Neo4j
			if (!songExists(songId) || !userExists(userName)) {
				return new DbQueryStatus("SONG OR USER DNE", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}

			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("songId", songId);
					params.put("playlistName", userName + "-favorites");
					
					
					// Query to check if the user has liked the song before
					String queryStr = "RETURN EXISTS ((:playlist {plName: $playlistName})-[:includes]->(:song {songId: $songId}))";
					StatementResult result = trans.run(queryStr, params);

					if (result.next().get(0).toString().equals("TRUE")) {
						
						// Query to remove the given song from the user's playlist
						queryStr = "MATCH (:playlist {plName: $playlistName})-[i:includes]->(:song {songId: $songId}) DELETE i";
						trans.run(queryStr, params);
						hasBeenLiked = true;
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
				if (hasBeenLiked) {
					queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
				} else {
					queryStatus = new DbQueryStatus("SONG NOT IN USER'S FAVORITES",
							DbQueryExecResult.QUERY_ERROR_GENERIC);
				}

			} else {
				queryStatus = new DbQueryStatus("FAILED TO CONNECT TO NEO4J", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
		}

		return queryStatus;
	}

	/**
	 * Removes a song node from the Neo4j database, including all of its relationships
	 * 
	 * @param songId the songId property of the song node in the Neo4j database
	 * @return the DbQueryStatus of the operation performed (OK for success, non OK o/w)
	 */
	@Override
	public DbQueryStatus deleteSongFromDb(String songId) {

		DbQueryStatus queryStatus;

		// Keeps track of whether the connection to Neo4j is successful
		boolean goodConnection = true;
		
		// Checks if the parameter is empty
		if (songId.isEmpty()) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			// Checks if the given song exists in Neo4j
			if (!songExists(songId)) {
				return new DbQueryStatus("SONG DNE", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}

			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("songId", songId);
					
					// Query to delete the song and all its relationships from Neo4j
					String queryStr = "MATCH (s:song) WHERE s.songId = $songId DETACH DELETE s";
					trans.run(queryStr, params);

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

	/**
	 * Checks if a song exists in the Neo4j database
	 * 
	 * @param songId the songId property of the song node in the Neo4j database
	 * @return true, if the song exists, false otherwise
	 */
	public boolean songExists(String songId) {
		boolean exists = false;

		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("songId", songId);

				String queryStr = "MATCH (s:song) WHERE s.songId = $songId return s";
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

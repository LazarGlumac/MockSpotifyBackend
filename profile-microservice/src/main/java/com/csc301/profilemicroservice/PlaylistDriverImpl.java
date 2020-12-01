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

	public DbQueryStatus addSong(String songId) {

		DbQueryStatus queryStatus;

		boolean goodConnection = true;

		if (songId == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			if (songExists(songId)) {
				return new DbQueryStatus("SONG DNE", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
			
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("songId", songId);

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

	@Override
	public DbQueryStatus likeSong(String userName, String songId) {

		DbQueryStatus queryStatus;

		boolean goodConnection = true;
		boolean alreadyLiked = false;

		if (userName == null || songId == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			if (!songExists(songId) || !userExists(userName)) {
				return new DbQueryStatus("SONG OR USER DNE", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
			
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("songId", songId);

					String queryStr = "MATCH (s:song) WHERE s.songId = $songId RETURN s";
					StatementResult result = trans.run(queryStr, params);

					if (result.hasNext()) {
						params.put("playlistName", userName + "-favorites");
						
						queryStr = "RETURN EXISTS ((:playlist {plName: $playlistName})-[:includes]->(:song {songId: $songId}))";
						result  = trans.run(queryStr, params);
						
						if (result.next().get(0).toString().equals("FALSE")) {
							queryStr = "MATCH (pl:playlist), (s:song) WHERE pl.plName = $playlistName AND s.songId = $songId MERGE (pl)-[:includes]->(s)";
							trans.run(queryStr, params);
						} else {
							alreadyLiked = true;
						}
						
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

	@Override
	public DbQueryStatus unlikeSong(String userName, String songId) {

		DbQueryStatus queryStatus;

		boolean goodConnection = true;
		boolean hasBeenLiked = false;

		if (userName == null || songId == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			if (!songExists(songId) || !userExists(userName)) {
				return new DbQueryStatus("SONG OR USER DNE", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
			
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("songId", songId);
					params.put("playlistName", userName + "-favorites");

					String queryStr = "RETURN EXISTS ((:playlist {plName: $playlistName})-[:includes]->(:song {songId: $songId}))";
					StatementResult result  = trans.run(queryStr, params);
					
					if (result.next().get(0).toString().equals("TRUE")) {
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
					queryStatus = new DbQueryStatus("SONG NOT IN USER'S FAVORITES", DbQueryExecResult.QUERY_ERROR_GENERIC);
				}
				
			} else {
				queryStatus = new DbQueryStatus("FAILED TO CONNECT TO NEO4J", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
		}

		return queryStatus;
	}

	@Override
	public DbQueryStatus deleteSongFromDb(String songId) {

		DbQueryStatus queryStatus;

		boolean goodConnection = true;

		if (songId == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			
			if (!songExists(songId)) {
				return new DbQueryStatus("SONG DNE", DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
			
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("songId", songId);

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
	
	public boolean userExists(String username) {
		boolean exists = false;
		
		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("username", username);

				String queryStr = "MATCH (p:profile) WHERE p.userName = $username return p";
				StatementResult result  = trans.run(queryStr, params);
				
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
	
	public boolean songExists(String songId) {
		boolean exists = false;
		
		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("songId", songId);

				String queryStr = "MATCH (s:song) WHERE s.songId = $songId return s";
				StatementResult result  = trans.run(queryStr, params);
				
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

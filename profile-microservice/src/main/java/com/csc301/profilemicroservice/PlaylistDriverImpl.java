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

	@Override
	public DbQueryStatus likeSong(String userName, String songId) {

		DbQueryStatus queryStatus;

		if (userName == null || songId == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("playlistName", userName+"-favorites");
					params.put("songId", songId);
					
					String queryStr = "MERGE (s:song {songId: $songId})";
					trans.run(queryStr, params);

					queryStr = "MATCH (pl:playlist), (s:song) WHERE pl.plName = $playlistName AND s.songId = $songId CREATE (pl)-[:includes]->(s)";
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
	public DbQueryStatus unlikeSong(String userName, String songId) {

		DbQueryStatus queryStatus;

		if (userName == null || songId == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("playlistName", userName+"-favorites");
					params.put("songId", songId);
					
					String queryStr = "MATCH (:playlist {plName: $playlistName})-[i:includes]->(:song {songId: $songId}) DELETE i";
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
	public DbQueryStatus deleteSongFromDb(String songId) {

		DbQueryStatus queryStatus;

		if (songId == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					Map<String, Object> params = new HashMap<String, Object>();
					params.put("songId", songId);
					
					String queryStr = "MATCH (s:song) WHERE s.songId = $songId DETACH DELETE s";
					trans.run(queryStr, params);

					trans.success();

				}
				session.close();
			}

			queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
		}

		return queryStatus;
	}
}

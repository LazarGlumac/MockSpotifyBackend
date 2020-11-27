package com.csc301.profilemicroservice;

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
					String queryStr = String.format("MERGE (s:song {songId: \"%1$s\"})", songId);
					trans.run(queryStr);

					queryStr = String.format(
							"MATCH (pl:playlist), (s:song) WHERE pl.plName = \"%1$s-favorites\" AND s.songId = \"%2$s\" CREATE (pl)-[:includes]->(s)",
							userName, songId);
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
	public DbQueryStatus unlikeSong(String userName, String songId) {

		DbQueryStatus queryStatus;

		if (userName == null || songId == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					String queryStr = String.format(
							"MATCH (:playlist {plName: \"%1$s-favorites\"})-[i:includes]->(:song {songId: \"%2$s\"}) DELETE i",
							userName, songId);
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
	public DbQueryStatus deleteSongFromDb(String songId) {

		DbQueryStatus queryStatus;

		if (songId == null) {
			queryStatus = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					String queryStr = String.format(
							"MATCH (s:song) WHERE s.songId = \"%1$s\" DETACH DELETE s",
							songId);
					trans.run(queryStr);

					trans.success();

				}
				session.close();
			}

			queryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
		}

		return queryStatus;
	}
}

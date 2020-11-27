package com.csc301.profilemicroservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

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
					String queryStr = String.format("CREATE (p:profile {userName: \"%1$s\", password: \"%2$s\"})", userName, password);
					trans.run(queryStr);
					
					queryStr = String.format("CREATE (p:playlist {plName: \"%1$s-favorites\"})", userName);
					trans.run(queryStr);
					
					queryStr = String.format("MATCH (p:profile), (pl:playlist) WHERE p.userName = \"%1$s\" AND pl.plName = \"%1$s-favorites\" CREATE (p)-[:created]->(pl)", userName);
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
		
		return null;
	}

	@Override
	public DbQueryStatus unfollowFriend(String userName, String frndUserName) {
		
		return null;
	}

	@Override
	public DbQueryStatus getAllSongFriendsLike(String userName) {
			
		return null;
	}
}

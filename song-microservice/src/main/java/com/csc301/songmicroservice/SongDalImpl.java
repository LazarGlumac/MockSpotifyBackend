package com.csc301.songmicroservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

@Repository
public class SongDalImpl implements SongDal {

	private final MongoTemplate db;

	@Autowired
	public SongDalImpl(MongoTemplate mongoTemplate) {
		this.db = mongoTemplate;
	}

	@Override
	public DbQueryStatus addSong(Song songToAdd) {

		boolean checkNull = songToAdd.getSongName() == null || songToAdd.getSongAlbum() == null || songToAdd.getSongArtistFullName() == null;
		DbQueryStatus toReturn;
		
		if (checkNull) {
			toReturn = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
		else {
			
			this.db.insert(songToAdd);
			toReturn = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
			toReturn.setData(songToAdd.getJsonRepresentation());
			
		}
		
		return toReturn;
	}

	@Override
	public DbQueryStatus findSongById(String songId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DbQueryStatus getSongTitleById(String songId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DbQueryStatus deleteSongById(String songId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DbQueryStatus updateSongFavouritesCount(String songId, boolean shouldDecrement) {
		// TODO Auto-generated method stub
		return null;
	}
}
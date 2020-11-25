package com.csc301.songmicroservice;

import java.util.List;

import org.bson.types.ObjectId;
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
		
		boolean checkNull = songId == null;
		DbQueryStatus toReturn;
		
		if (checkNull) {
			toReturn = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} 
		else {

			Query query = new Query();
			query.addCriteria(Criteria.where("_id").is(songId));
			
			List<Song> songToFind = this.db.find(query, Song.class);
			
			if (songToFind.size() == 0) {
				toReturn = new DbQueryStatus("NOT_FOUND", DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
			}
			else {
				toReturn = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
				toReturn.setData(songToFind.get(0).getJsonRepresentation());
			}
			
		}
		
		return toReturn;
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
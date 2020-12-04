package com.csc301.songmicroservice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	
	/**
	 * Handles inserting a Song object into the MongoDB, checking whether all parameters are non-empty.
	 * 
	 * @param songToAdd the song to be inserted
	 * @return the DbQueryStatus of the operation performed (OK for success, non OK o/w)
	 */
	@Override
	public DbQueryStatus addSong(Song songToAdd) {

		boolean checkNull = songToAdd.getSongName().isEmpty() || songToAdd.getSongAlbum().isEmpty()
				|| songToAdd.getSongArtistFullName().isEmpty();
		DbQueryStatus toReturn;

		if (checkNull) {
			toReturn = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {

			this.db.insert(songToAdd);
			toReturn = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
			toReturn.setData(songToAdd.getJsonRepresentation());

		}

		return toReturn;
	}

	/**
	 * Handles finding a Song object in the MongoDB given its ObjectID.
	 * 
	 * @param songId the ObjectID of the song in the database
	 * @return the DbQueryStatus of the operation performed (OK for success, non OK o/w), containing all properties of the song
	 */
	@Override
	public DbQueryStatus findSongById(String songId) {

		boolean checkNull = songId.isEmpty();
		DbQueryStatus toReturn;

		if (checkNull) {
			toReturn = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {

			Query query = new Query();
			query.addCriteria(Criteria.where("_id").is(songId));

			List<Song> songToFind = this.db.find(query, Song.class);

			if (songToFind.size() == 0) {
				toReturn = new DbQueryStatus("NOT_FOUND", DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
			} else {
				toReturn = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
				toReturn.setData(songToFind.get(0).getJsonRepresentation());
			}

		}

		return toReturn;
	}
	
	/**
	 * Handles finding a Song object's title in the MongoDB given its ObjectID.
	 * 
	 * @param songId the ObjectID of the song in the database
	 * @return the DbQueryStatus of the operation performed (OK for success, non OK o/w), containing the title of the song
	 */
	@Override
	public DbQueryStatus getSongTitleById(String songId) {

		DbQueryStatus songFromID = findSongById(songId);
		DbQueryStatus toReturn;

		if (!(songFromID.getMessage().equals("OK"))) {
			toReturn = new DbQueryStatus(songFromID.getMessage(), songFromID.getdbQueryExecResult());
		} else {

			toReturn = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
			toReturn.setData(((Map<String, String>) songFromID.getData()).get("songName"));

		}

		return toReturn;
	}
	
	/**
	 * Handles deleting a song from the MongoDB given its ObjectID.
	 * 
	 * @param songId the ObjectID of the song in the database
	 * @return the DbQueryStatus of the operation performed (OK for success, non OK o/w)
	 */
	@Override
	public DbQueryStatus deleteSongById(String songId) {

		boolean checkNull = songId.isEmpty();
		DbQueryStatus toReturn;

		if (checkNull) {
			toReturn = new DbQueryStatus("MISSING BODY PARAMETER", DbQueryExecResult.QUERY_ERROR_GENERIC);
		} else {
			Query query = new Query();
			query.addCriteria(Criteria.where("_id").is(songId));

			long deletedResult = this.db.remove(query, Song.class).getDeletedCount();

			if (deletedResult == 0) {
				toReturn = new DbQueryStatus("NOT_FOUND", DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
			} else {
				toReturn = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
			}

		}

		return toReturn;
	}
	
	/**
	 * Handles incrementing/decrementing the favourites count of a song in the MongoDB.
	 * 
	 * @param songId the ObjectID of the song in the database
	 * @param shouldDecrement the boolean value of whether the count should be incremented or decremented
	 * @return the DbQueryStatus of the operation performed (OK for success, non OK o/w)
	 */
	@Override
	public DbQueryStatus updateSongFavouritesCount(String songId, boolean shouldDecrement) {

		DbQueryStatus songFromID = findSongById(songId);
		DbQueryStatus toReturn;

		if (!(songFromID.getMessage().equals("OK"))) {
			toReturn = new DbQueryStatus(songFromID.getMessage(), songFromID.getdbQueryExecResult());
		} else {

			toReturn = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);

			HashMap<String, String> mapFromQuery = (HashMap<String, String>) songFromID.getData();

			Song songFromQuery = new Song(mapFromQuery.get("songName"), mapFromQuery.get("songArtistFullName"),
					mapFromQuery.get("songAlbum"));
			songFromQuery.setSongAmountFavourites(Long.valueOf(mapFromQuery.get("songAmountFavourites")));
			songFromQuery.setId(new ObjectId(mapFromQuery.get("id")));

			int change;

			if (shouldDecrement) {
				change = -1;
			} else {
				change = 1;
			}

			if (songFromQuery.getSongAmountFavourites() + change < 0) {
				toReturn = new DbQueryStatus("INVALID_OPERATION", DbQueryExecResult.QUERY_ERROR_GENERIC);
			} else {
				songFromQuery.setSongAmountFavourites(songFromQuery.getSongAmountFavourites() + change);
				this.db.save(songFromQuery);

				toReturn = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
			}

		}

		return toReturn;
	}

}
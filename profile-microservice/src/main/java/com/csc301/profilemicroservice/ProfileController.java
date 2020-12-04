package com.csc301.profilemicroservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/")
public class ProfileController {
	public static final String KEY_USER_NAME = "userName";
	public static final String KEY_USER_FULLNAME = "fullName";
	public static final String KEY_USER_PASSWORD = "password";
	public static final String KEY_FRIEND_USERNAME = "friendUserName";
	public static final String KEY_SONGID = "songId";

	@Autowired
	private final ProfileDriverImpl profileDriver;

	@Autowired
	private final PlaylistDriverImpl playlistDriver;

	OkHttpClient client = new OkHttpClient();

	public ProfileController(ProfileDriverImpl profileDriver, PlaylistDriverImpl playlistDriver) {
		this.profileDriver = profileDriver;
		this.playlistDriver = playlistDriver;
	}

	/**
	 * Controls and handles the route for /profile (POST), adding the new profile to the Neo4j database
	 * 
	 * @param params a map of the query parameters required for this route (userName: the username of the new user, fullName:
	 * the full name of the new user, and password: the user's password)
	 * @param request the HttpServlet representing from where the request was sent
	 * @return the response body for the request, which contains the status and path of the request
	 */
	@RequestMapping(value = "/profile", method = RequestMethod.POST)
	public @ResponseBody Map<String, Object> addProfile(@RequestParam Map<String, String> params,
			HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("POST %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = profileDriver.createUserProfile(params.get(KEY_USER_NAME),
				params.get(KEY_USER_FULLNAME), params.get(KEY_USER_PASSWORD));

		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}

	/**
	 * Controls and handles the route for /followFriend/{userName}/{friendUserName} (PUT), adding a "follows" relationship
	 * from the given user to its friend in the Neo4j database
	 * 
	 * @param userName the userName property of the user in the Neo4j database
	 * @param friendUserName the userName property of the user's friend in the Neo4j database
	 * @param request the HttpServlet representing from where the request was sent
	 * @return the response body for the request, which contains the status and path of the request
	 */
	@RequestMapping(value = "/followFriend/{userName}/{friendUserName}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> followFriend(@PathVariable(KEY_USER_NAME) String userName,
			@PathVariable(KEY_FRIEND_USERNAME) String friendUserName, HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = profileDriver.followFriend(userName, friendUserName);

		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}

	/**
	 * Controls and handles the route for /getAllFriendFavouriteSongTitles/{userName} (GET)
	 * 
	 * @param userName the userName property of the user in the Neo4j database
	 * @param request the HttpServlet representing from where the request was sent
	 * @return the response body for the request, which contains the status, path, and data (a map of the user's friends and their
	 * liked songs) of the request
	 */
	@RequestMapping(value = "/getAllFriendFavouriteSongTitles/{userName}", method = RequestMethod.GET)
	public @ResponseBody Map<String, Object> getAllFriendFavouriteSongTitles(
			@PathVariable(KEY_USER_NAME) String userName, HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = profileDriver.getAllSongFriendsLike(userName);
		DbQueryStatus finalDbQueryStatus;

		// Checking if the Neo4j queries to find all the liked songs of the user's friends was successful
		if (dbQueryStatus.getdbQueryExecResult().equals(DbQueryExecResult.QUERY_OK)) {
			
			// Storing the liked songs of the user's friends
			Object data = dbQueryStatus.getData();

			ObjectMapper mapper = new ObjectMapper();
			
			/* Storing the liked songs of the user's friends in a map where the friend's usernames are the keys
			 * and the values are the lists of liked songs for each friend*/
			Map<String, Object> friendsFavoriteSongTitles = mapper.convertValue(data, Map.class);
			
			// Iterator to iterate through friendsFavoriteSongTitles map in order to find the title of each liked song
			Iterator<Entry<String, Object>> mapIterator = friendsFavoriteSongTitles.entrySet().iterator();
			
			// Keeps track of whether the call to the Song Microservice is successful
			boolean goodCall = true;

			// Iterating through the friendsFavoriteSongTitles map to find the titles of the liked songs of each friend
			while (mapIterator.hasNext() && goodCall) {
				
				// Stores the current entry of the map
				Map.Entry friendName = (Map.Entry) mapIterator.next();
				
				// Storing the list of songs liked by the current friend
				String friendsSongIds = friendsFavoriteSongTitles.get(friendName.getKey()).toString();
				friendsSongIds = friendsSongIds.substring(1, friendsSongIds.length() - 1);

				if (!friendsSongIds.isEmpty()) {
					// Converting friendsSongIds into an iterable array
					String[] songIds = friendsSongIds.split(", ");
					
					// Stores the titles of each liked song
					List<String> songTitles = new ArrayList<String>();
					
					// Iterating through each songId in songIds and making a request to the Song Microservice to find the song title
					for (String songId : songIds) {
						try {
							String url = String.format("http://localhost:3001/getSongTitleById/%s", songId);

							Request okRequest = new Request.Builder().url(url).method("GET", null).build();

							Call call = client.newCall(okRequest);

							Response responseGSTBI = null;

							responseGSTBI = call.execute();
							String getSongIdBody = responseGSTBI.body().string();

							// Checking if a song title was found
							if (mapper.readValue(getSongIdBody, Map.class).get("status").toString().equals("OK")) {
								
								// Adding the found song title to songTitles
								songTitles.add(mapper.readValue(getSongIdBody, Map.class).get("data").toString());
							} else {
								if (!mapper.readValue(getSongIdBody, Map.class).get("status").toString()
										.equals("NOT_FOUND")) {
									goodCall = false;
									break;
								}
							}
						} catch (Exception e) {
							goodCall = false;
							break;
						}
					}
					
					// Adding the friend-songs, key-value pair to friendsFavoriteSongTitles 
					friendsFavoriteSongTitles.put(friendName.getKey().toString(), songTitles);
				} else {
					friendsFavoriteSongTitles.put(friendName.getKey().toString(), new String [0]);
				}
			}

			if (goodCall) {
				finalDbQueryStatus = new DbQueryStatus("OK", DbQueryExecResult.QUERY_OK);
				finalDbQueryStatus.setData(friendsFavoriteSongTitles);
			} else {
				finalDbQueryStatus = new DbQueryStatus("FAILED TO MAKE REQUEST TO MONGODB",
						DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
		} else {
			finalDbQueryStatus = new DbQueryStatus("FAILED TO MAKE REQUEST TO NEO4J",
					DbQueryExecResult.QUERY_ERROR_GENERIC);
		}

		response = Utils.setResponseStatus(response, finalDbQueryStatus.getdbQueryExecResult(),
				finalDbQueryStatus.getData());

		return response;
	}

	/**
	 * Controls and handles the route for /unfollowFriend/{userName}/{friendUserName} (PUT), removing the "follows" relationship
	 * from the given user to its friend in the Neo4j database
	 * 
	 * @param userName the userName property of the user in the Neo4j database
	 * @param friendUserName the userName property of the user's friend in the Neo4j database
	 * @param request the HttpServlet representing from where the request was sent
	 * @return the response body for the request, which contains the status and path of the request
	 */
	@RequestMapping(value = "/unfollowFriend/{userName}/{friendUserName}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> unfollowFriend(@PathVariable(KEY_USER_NAME) String userName,
			@PathVariable(KEY_FRIEND_USERNAME) String friendUserName, HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = profileDriver.unfollowFriend(userName, friendUserName);

		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}

	
	/**
	 * Controls and handles the route for /addSong/{songId} (PUT), adding a song node to the Neo4j database
	 * 
	 * @param songId the songId property of the new song node
	 * @param request the HttpServlet representing from where the request was sent
	 * @return the response body for the request, which contains the status and path of the request
	 */
	@RequestMapping(value = "/addSong/{songId}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> addSong(@PathVariable(KEY_SONGID) String songId,
			HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = playlistDriver.addSong(songId);

		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}

	/**
	 * Controls and handles the route for /likeSong/{userName}/{songId} (PUT), adding an "includes" relationship from the 
	 * user's playlist to the given song in Neo4j
	 * 
	 * @param userName the userName property of the user in the Neo4j database
	 * @param songId the songId property of the song in the Neo4j database
	 * @param request the HttpServlet representing from where the request was sent
	 * @return the response body for the request, which contains the status and path of the request
	 */
	@RequestMapping(value = "/likeSong/{userName}/{songId}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> likeSong(@PathVariable(KEY_USER_NAME) String userName,
			@PathVariable(KEY_SONGID) String songId, HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = playlistDriver.likeSong(userName, songId);
		
		// Keeps track of whether the call to Song Microservice is successful
		boolean goodCall = true;
		
		// Checking if the song was successfully liked in Neo4j
		if (dbQueryStatus.getMessage().equals("OK")) {
			
			// Incrementing songAmountFavourites in MongoDB
			try {
				String urlParams = String.format("http://localhost:3001/updateSongFavouritesCount/%s", songId);

				HttpUrl.Builder urlBuilder = HttpUrl.parse(urlParams).newBuilder();
				urlBuilder.addQueryParameter("shouldDecrement", "false");
				String url = urlBuilder.build().toString();

				RequestBody body = RequestBody.create(new byte[0], null);

				Request okRequest = new Request.Builder().url(url).method("PUT", body).build();

				Call call = client.newCall(okRequest);

				call.execute();

			} catch (Exception e) {
				goodCall = false;
			}
		}

		if (!goodCall) {
			dbQueryStatus = new DbQueryStatus("FAILED TO MAKE REQUEST TO MONGODB",
					DbQueryExecResult.QUERY_ERROR_GENERIC);
		}

		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}

	
	/**
	 * Controls and handles the route for /unlikeSong/{userName}/{songId} (PUT), removing the "includes" relationship from the 
	 * user's playlist to the given song in Neo4j
	 * 
	 * @param userName the userName property of the user in the Neo4j database
	 * @param songId the songId property of the song in the Neo4j database
	 * @param request the HttpServlet representing from where the request was sent
	 * @return the response body for the request, which contains the status and path of the request
	 */
	@RequestMapping(value = "/unlikeSong/{userName}/{songId}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> unlikeSong(@PathVariable(KEY_USER_NAME) String userName,
			@PathVariable(KEY_SONGID) String songId, HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = playlistDriver.unlikeSong(userName, songId);
		
		// Keeps track of whether the call to the Song Microservices is successful
		boolean goodCall = true;
		
		// Checking if the song was successfully unliked in Neo4j
		if (dbQueryStatus.getdbQueryExecResult().equals(DbQueryExecResult.QUERY_OK)) {
			
			// Decrementing songAmountFavourites in MongoDB
			try {
				String urlParams = String.format("http://localhost:3001/updateSongFavouritesCount/%s", songId);

				HttpUrl.Builder urlBuilder = HttpUrl.parse(urlParams).newBuilder();
				urlBuilder.addQueryParameter("shouldDecrement", "true");
				String url = urlBuilder.build().toString();

				RequestBody body = RequestBody.create(new byte[0], null);

				Request okRequest = new Request.Builder().url(url).method("PUT", body).build();

				Call call = client.newCall(okRequest);

				call.execute();

			} catch (Exception e) {
				goodCall = false;
			}
		}

		if (!goodCall) {
			dbQueryStatus = new DbQueryStatus("FAILED TO MAKE REQUEST TO MONGODB",
					DbQueryExecResult.QUERY_ERROR_GENERIC);
		}

		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}

	/**
	 * Controls and handles the route for /deleteAllSongsFromDb/{songId} (PUT), removing the given song node and all its 
	 * relationships in the Neo4j database
	 * 
	 * @param songId the songId property of the song in the Neo4j database
	 * @param request the HttpServlet representing from where the request was sent
	 * @return the response body for the request, which contains the status and path of the request
	 */
	@RequestMapping(value = "/deleteAllSongsFromDb/{songId}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> deleteAllSongsFromDb(@PathVariable(KEY_SONGID) String songId,
			HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("PUT %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = playlistDriver.deleteSongFromDb(songId);

		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}
}
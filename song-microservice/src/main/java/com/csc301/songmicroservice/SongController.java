package com.csc301.songmicroservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/")
public class SongController {

	@Autowired
	private final SongDal songDal;

	OkHttpClient client = new OkHttpClient();

	public SongController(SongDal songDal) {
		this.songDal = songDal;
	}


	@RequestMapping(value = "/getSongById/{songId}", method = RequestMethod.GET)
	public @ResponseBody Map<String, Object> getSongById(@PathVariable("songId") String songId,
			HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("GET %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = songDal.findSongById(songId);

		// 		response.put("message", dbQueryStatus.getMessage());
		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}


	@RequestMapping(value = "/getSongTitleById/{songId}", method = RequestMethod.GET)
	public @ResponseBody Map<String, Object> getSongTitleById(@PathVariable("songId") String songId,
			HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("GET %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = songDal.getSongTitleById(songId);

		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}


	@RequestMapping(value = "/deleteSongById/{songId}", method = RequestMethod.DELETE)
	public @ResponseBody Map<String, Object> deleteSongById(@PathVariable("songId") String songId,
			HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("DELETE %s", Utils.getUrl(request)));

		DbQueryStatus dbQueryStatus = songDal.deleteSongById(songId);

		if (dbQueryStatus.getdbQueryExecResult() == DbQueryExecResult.QUERY_OK) {
			
			String path = String.format("http://localhost:3002/deleteAllSongsFromDb/%s", songId);

			Request okRequest = new Request.Builder().url(path).method("PUT", RequestBody.create(new byte[0], null)).build();

			Call call = client.newCall(okRequest);

			Response responseFromPMS = null;

			try {
				responseFromPMS = call.execute();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		response = Utils.setResponseStatus(response, dbQueryStatus.getdbQueryExecResult(), dbQueryStatus.getData());

		return response;
	}


	@RequestMapping(value = "/addSong", method = RequestMethod.POST)
	public @ResponseBody Map<String, Object> addSong(@RequestParam Map<String, String> params,
			HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("path", String.format("POST %s", Utils.getUrl(request)));

		String songName = params.get("songName");
		String songArtistFullName = params.get("songArtistFullName");
		String songAlbum = params.get("songAlbum");

		Song songToAdd =  new Song(songName, songArtistFullName, songAlbum);
		DbQueryStatus statusResult = this.songDal.addSong(songToAdd);
		
		if (statusResult.getdbQueryExecResult() == 	DbQueryExecResult.QUERY_OK) {
			
			String songID = ((Map<String, String>) (statusResult.getData())).get("id");
			
			String path = String.format("http://localhost:3002/addSong/%s", songID);

			Request okRequest = new Request.Builder().url(path).method("PUT", RequestBody.create(new byte[0], null)).build();

			Call call = client.newCall(okRequest);

			Response responseFromPMS = null;

			try {
				responseFromPMS = call.execute();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		
		response = Utils.setResponseStatus(response, statusResult.getdbQueryExecResult(), statusResult.getData());

		return response;

	}


	@RequestMapping(value = "/updateSongFavouritesCount/{songId}", method = RequestMethod.PUT)
	public @ResponseBody Map<String, Object> updateFavouritesCount(@PathVariable("songId") String songId,
			@RequestParam("shouldDecrement") String shouldDecrement, HttpServletRequest request) {

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("data", String.format("PUT %s", Utils.getUrl(request)));

		boolean decrementAsBoolean = shouldDecrement.equals("true");

		DbQueryStatus statusResult = this.songDal.updateSongFavouritesCount(songId, decrementAsBoolean);
		response = Utils.setResponseStatus(response, statusResult.getdbQueryExecResult(), statusResult.getData());

		return response;
	}
}
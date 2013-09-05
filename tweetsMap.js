function tweetsMap(){

	//find words in the document text
	var words = this.geotweets.match(/\w+/g);
	//var words = this.geotweets.match(/\w+g);
	
	if (words == null){
		return;
	
	}
	
	for (var i = 0; i < words.length; i++){
		//emit every word, with count of one
		emit(words[i], {count: 1});
	
	}


}

function geotimeMap(){

	var mydatetime = this.created_at;

	// it may not be correct for negative numbers. should -4.6454/30 give 0 or -1?
	var mylon = Math.floor(this.coordinates.coordinates[0]/30); 
	var mylat = Math.floor(this.coordinates.coordinates[1]/30);

	var created_at_minute = new Date(mydatetime.getFullYear(),
                                     mydatetime.getMonth(),
                                     mydatetime.getDate(),
                                     mydatetime.getHours(),
                                     mydatetime.getMinutes());

	emit({created_at_min:created_at_minute, longitude:mylon, latitude:mylat}, {count: 1});
	}

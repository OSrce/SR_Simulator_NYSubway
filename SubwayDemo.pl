#!/usr/bin/perl

# load module
use DBI;
use POSIX qw/strftime/;
use Date::Calc qw(Add_Delta_DHMS);

my $timeinterval = 5;

# connect to database
my $dbh = DBI->connect("DBI:Pg:dbname=sr_data;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});

while(1){
	sleep($timeinterval);

	#Get the time
	($s1,$m1,$h1,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
	$timeholder = sprintf ("%02d%02d%02d", $h1, $m1, $s1);
	my $timeinseconds = 60*$m1 + 3600*$h1 + $s1;


	#print "wday is $wday\n";
	if ($wday < 6) {
		$daysearch = "WKD";
	}	elsif ($wday = 6) {
		$daysearch = "SAT";
	} else {
		$daysearch = "SUN";
	}

	#Select all the trains that are active at this time (i.e. between two stops)
	#find the stations that each subway is between
	# PREPARE THE QUERY

	$query = "select a.trip_id, a.departure_time, b.arrival_time, x.stop_lon, x.stop_lat, y.stop_lon, y.stop_lat, a.stop_id, b.stop_id  from (select departure_time, trip_id, stop_sequence, stop_id from nyc_subways) a, (select arrival_time, trip_id, stop_sequence, stop_id from nyc_subways) b, (select stop_lat, stop_lon, stop_id from nyc_subway_stops) x,  (select stop_lat, stop_lon, stop_id from nyc_subway_stops) y where a.trip_id=b.trip_id and (b.stop_sequence-a.stop_sequence)=1 and a.departure_time <= '$timeholder' and b.arrival_time>='$timeholder' AND x.stop_id=a.stop_id AND y.stop_id=b.stop_id AND a.trip_id ~ '$daysearch'  ";

#and (a.trip_id='B20121216WKD_117100_M..N20R' OR a.trip_id='B20121216WKD_120750_L..N01R')



	#print "$daysearch\n";

	$query_handle = $dbh->prepare($query);

	# EXECUTE THE QUERY
	$query_handle->execute();

	# BIND TABLE COLUMNS TO VARIABLES
	$query_handle->bind_columns(undef, \$tripid, \$depart, \$arrive, \$start_lon, \$start_lat, \$end_lon, \$end_lat, \$startid, \$endid);

	# LOOP THROUGH RESULTS
	while($query_handle->fetch()) {

		
	#FOR finding geometries of a subway route
	#SELECT sr_geom from sr_layer_static_data where layer_id=2002 AND feature_data LIKE '%"ROUTE":"%5%","NAME"%';


	#calculate what percent along the way the subway is
		#parse the time string e.g. 10:15:12 to get the time in seconds from midnight (for easier math)
		@depart_tokens = split(/:/, "$depart");
		@arrive_tokens = split(/:/, "$arrive");
		my $depart_seconds =   @depart_tokens[0] * 3600 + @depart_tokens[1]*60 + @depart_tokens[2];
		my $arrive_seconds  = @arrive_tokens[0] * 3600 + @arrive_tokens[1]*60 + @arrive_tokens[2];

		my $trip_interval = $arrive_seconds - $depart_seconds;
		my $pct_along_route =  ($timeinseconds - $depart_seconds)/($arrive_seconds - $depart_seconds);
		my $num = $timeinseconds - $depart_seconds;

		#get the subway line from the ID
		my $subwayline = substr($tripid, 20, 1);
		#Some subways have 2 digit IDs, but I'll ignore that for now
		#my $subwaylinetmp2 = substr($tripid, 21, 1);


		#get the route for that subway
		#interpolate it's position along the route	

		$routequery = "SELECT st_astext(st_line_interpolate_point(sr_geom, $pct_along_route)), id from sr_layer_static_data where layer_id=2002 AND feature_data LIKE '%\"ROUTE\":\"%$subwayline%\",\"NAME\"%'  AND (ST_Distance(st_startpoint(sr_geom),st_geomfromtext('POINT($start_lon $start_lat)', 4326)) < 0.001 AND ST_Distance(st_endpoint(sr_geom),st_geomfromtext('POINT($end_lon $end_lat)', 4326)) < 0.001) OR (ST_Distance(st_endpoint(sr_geom),st_geomfromtext('POINT($start_lon $start_lat)', 4326)) < 0.001 AND ST_Distance(st_startpoint(sr_geom),st_geomfromtext('POINT($end_lon $end_lat)', 4326)) < 0.001)";

		#print "$routequery\n";

		$routequery_handle = $dbh->prepare($routequery);

		# EXECUTE THE QUERY
		$routequery_handle->execute();

		# BIND TABLE COLUMNS TO VARIABLES
		$routequery_handle->bind_columns(undef, \$train, \$dataid );

		$routequery_handle->fetch();

	
		print "$tripid $dataid $pct_along_route $startid $endid		$subwayline  $train\n";

		#update the sr tables
		#insert into sr_locations (source, address) values(6, 'Train test') returning id;
		#insert into entity_status (entity_id, location_id, data) values (1, 10, 'Train is a comin');
		#insert into entity (name, last_status_id, last_location_status_id) values ('testtrain', 10, 10);

		$insertlocations = "insert into sr_locations (source, address) values(6, 'Train test') returning id";
		$insertlocations_handle = $dbh->prepare($insertlocations);
		# EXECUTE THE QUERY
		$insertlocations_handle->execute();
		#get the returned id
		$locid = $insertlocations_handle->fetch()->[0];


		### BEGIN CHECK ENTITY TO SEE IF IT ALREADY HAS A last_location_status_id and if so set the data_end value to now.
		$query = "select id, last_location_status_id from entity where name='$tripid'";

		$query2_handle = $dbh->prepare($query);

		# EXECUTE THE QUERY
		$query2_handle->execute();

		# BIND TABLE COLUMNS TO VARIABLES
		$query2_handle->bind_columns(\$entity_id, \$last_loc_status_id );
		$query2_handle->fetch();

		print "need to update entity_status,  last_location_status_id = $last_loc_status_id \n";

		### END CHECK EXISTING ENTITY VALUES






		$insertstatus = "insert into entity_status (entity_id, has_data, has_begin, location_id, data, data_begin) values ($entity_id, 'TRUE', 'TRUE', $locid, '{ \"train_info\": \"The $subwayline train is a comin at $timeholder\" } ', now() ) returning id";

		$insertstatus_handle = $dbh->prepare($insertstatus);
		$insertstatus_handle->execute();
		#get the returned status id
		$statusid = $insertstatus_handle->fetch()->[0];


	### BEGIN UPDATE entity_status set data_end to now().
	if($last_loc_status_id != -1) {
		my $rows = $dbh->do("UPDATE entity_status set data_end=now(), has_end='TRUE' WHERE id=$last_loc_status_id" );
	}
	### END UPDATE entity_status
		

	my $rows = $dbh->do("UPDATE entity set has_location='TRUE', location_id=$locid, last_location_status_id=$statusid where id=$entity_id");


	} 




	#my $rows = $dbh->do("select a.trip_id, a.departure_time, b.arrival_time from (select departure_time, trip_id, stop_sequence from nyc_subways) a, (select arrival_time, trip_id, stop_sequence from 
	#nyc_subways) b where a.trip_id=b.trip_id and (b.stop_sequence-a.stop_sequence)=1 and a.departure_time < '$timeholder' and b.arrival_time>'$timeholder' limit 1;");
	
	#print "$rows\n"

}#end big for loop



# clean up
#$dbh->disconnect();


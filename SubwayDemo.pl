#!/usr/bin/perl

# load module
use DBI;
use POSIX qw/strftime/;
use Date::Calc qw(Add_Delta_DHMS);

my $timeinterval = 2;

# connect to database
my $dbh = DBI->connect("DBI:Pg:dbname=sr_data;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});
my $srdb = DBI->connect("DBI:Pg:dbname=sitrep;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});

while(1){
	#sleep($timeinterval);

	#Get the time
	($s1,$m1,$h1,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
	
	#To run this from five hours in the past
	#($year2, $month2, $day2, $h2, $m2, $s2) = Add_Delta_DHMS( 1900, 02, 23, $h1, $m1, $s1, 0, -5, 0, 0);
	#my $timeholder = sprintf ("%02d%02d%02d", $h2, $m2, $s2);
	#my $timeinseconds = 60*$m2 + 3600*$h2 + $s2;
	
	$timeholder = sprintf ("%02d%02d%02d", $h1, $m1, $s1);
	my $timeinseconds = 60*$m1 + 3600*$h1 + $s1;

#	($year2, $month2, $day2, $h2, $m2, $s2) = Add_Delta_DHMS( 1900, 02, 23, $h1, $m1, $s1, 0, 0, 0, -2*$timeinterval);
#	my $timeholderpast = sprintf ("%02d%02d%02d", $h2, $m2, $s2);
	



	#print "wday is $wday\n";
	if ($wday < 6) {
		$daysearch = "WKD";
	}	elsif ($wday = 6) {
		$daysearch = "SAT";
	} else {
		$daysearch = "SUN";
	}


	#Find a train for each trip
	#SELECT * FROM entity WHERE data @> '"type"=>"train"'::hstore;


	#Select all the trains that are active at this time (i.e. between two stops)
	#find the stations that each subway is between
	# PREPARE THE QUERY

	#regular
	#$query = "select a.trip_id, a.departure_time, b.arrival_time, x.stop_lon, x.stop_lat, y.stop_lon, y.stop_lat, a.stop_id, b.stop_id  from (select departure_time, trip_id, stop_sequence, stop_id from nyc_subways) a, (select arrival_time, trip_id, stop_sequence, stop_id from nyc_subways) b, (select stop_lat, stop_lon, stop_id from nyc_subway_stops) x,  (select stop_lat, stop_lon, stop_id from nyc_subway_stops) y where a.trip_id=b.trip_id and (b.stop_sequence-a.stop_sequence)=1 and a.departure_time <= '$timeholder' and b.arrival_time>='$timeholder' AND x.stop_id=a.stop_id AND y.stop_id=b.stop_id AND a.trip_id ~ '$daysearch'";
	#This looks for recently completed trips as well (because arrival time only has to be after about 10 seoconds in the past
	#$query = "select distinct a.trip_id, a.departure_time, b.arrival_time, x.stop_lon, x.stop_lat, y.stop_lon, y.stop_lat, a.stop_id, b.stop_id  from (select departure_time, trip_id, stop_sequence, stop_id from nyc_subways) a, (select arrival_time, trip_id, stop_sequence, stop_id from nyc_subways) b, (select stop_lat, stop_lon, stop_id from nyc_subway_stops) x,  (select stop_lat, stop_lon, stop_id from nyc_subway_stops) y where a.trip_id=b.trip_id and (b.stop_sequence-a.stop_sequence)=1 and a.departure_time <= '$timeholder' and b.arrival_time>='$timeholderpast' AND x.stop_id=a.stop_id AND y.stop_id=b.stop_id AND a.trip_id ~ '$daysearch' order by arrival_time desc";
	$query = "select a.trip_id, a.departure_time, b.arrival_time, x.stop_lon, x.stop_lat, y.stop_lon, y.stop_lat, a.stop_id, b.stop_id from (select departure_time, trip_id, stop_sequence, stop_id from nyc_subways) a, (select arrival_time, trip_id, stop_sequence, stop_id from nyc_subways) b, (select stop_lat, stop_lon, stop_id from nyc_subway_stops) x,  (select stop_lat, stop_lon, stop_id from nyc_subway_stops) y where a.trip_id=b.trip_id and (b.stop_sequence-a.stop_sequence)=1 and a.departure_time <= '$timeholder' and b.arrival_time>='$timeholder' AND x.stop_id=a.stop_id AND y.stop_id=b.stop_id AND a.trip_id ~ '$daysearch'";
	


	#$query = "select distinct a.trip_id, a.departure_time, b.arrival_time, x.stop_lon, x.stop_lat, y.stop_lon, y.stop_lat, a.stop_id, b.stop_id  from (select departure_time, trip_id, stop_sequence, stop_id from nyc_subways) a, (select arrival_time, trip_id, stop_sequence, stop_id from nyc_subways) b, (select stop_lat, stop_lon, stop_id from nyc_subway_stops) x,  (select stop_lat, stop_lon, stop_id from nyc_subway_stops) y where a.trip_id=b.trip_id and (b.stop_sequence-a.stop_sequence)=1 and a.departure_time <= '$timeholder' and b.arrival_time>='$timeholder' AND x.stop_id=a.stop_id AND y.stop_id=b.stop_id AND a.trip_id ~ '$daysearch' group by b.arrival_time, a.trip_id, a.departure_time, x.stop_lon, x.stop_lat, y.stop_lon, y.stop_lat, a.stop_id, b.stop_id  order by b.arrival_time desc";
	
	#print "Query is $query\n";
	
	$query_handle = $dbh->prepare($query);

	# EXECUTE THE QUERY
	$query_handle->execute();

	# BIND TABLE COLUMNS TO VARIABLES
	$query_handle->bind_columns(undef, \$tripid, \$depart, \$arrive, \$start_lon, \$start_lat, \$end_lon, \$end_lat, \$startid, \$endid);

	# LOOP THROUGH RESULTS (i.e. all the current train trips)
			$counter=0;	
	while($query_handle->fetch()) {

	#calculate what percent along the way the subway is
		#parse the time string e.g. 10:15:12 to get the time in seconds from midnight (for easier math)
		@depart_tokens = split(/:/, "$depart");
		@arrive_tokens = split(/:/, "$arrive");
		my $depart_seconds =   @depart_tokens[0] * 3600 + @depart_tokens[1]*60 + @depart_tokens[2];
		my $arrive_seconds  = @arrive_tokens[0] * 3600 + @arrive_tokens[1]*60 + @arrive_tokens[2];



		my $trip_interval = $arrive_seconds - $depart_seconds;
		my $pct_along_route =  ($timeinseconds - $depart_seconds)/($arrive_seconds - $depart_seconds);
		my $num = $timeinseconds - $depart_seconds;
		
		my $direction = substr($tripid, 23, 1);
		
		if($direction eq 'S'){
			$pct_along_route = 1- $pct_along_route;
		}
		
		#print "$arrive_seconds - $depart_seconds is $trip_interval and the time is $timeinseconds \n";
		#print "$start_lon $start_lat to $end_lon $end_lat . From $startid to $endid\n";
	
		#FIX THIS! This is because my "present time" actually extends a few seconds into the past. I did that because I want trips to update all the way to the end. I'm not sure of the best way
		#to deal with this unpleasant side effect.
	#	if ($pct_along_route <= 1) {
	#		print "pct_along_route is $pct_along_route \n";	
	#		$pct_along_route = 1;
	#	}	
	
	#get rid of this
	#	$pct_along_route = 0.99;
	
		#get the subway line from the ID
		my $subwayline = substr($tripid, 20, 2);
		#Some subways have 2 digit IDs, but I'll ignore that for now
		#my $subwaylinetmp2 = substr($tripid, 21, 1);


		#get the route for that subway
		#interpolate it's position along the route	

		#Note: this query might require accessing tables from different databases
		#$routequery = "SELECT st_astext(st_line_interpolate_point(sr_geom, $pct_along_route)), id from sr_layer_static_data where layer_id=2002 AND feature_data LIKE '%\"ROUTE\":\"%$subwayline%\",\"NAME\"%'  AND (ST_Distance(st_startpoint(sr_geom),st_geomfromtext('POINT($start_lon $start_lat)', 4326)) < 0.001 AND ST_Distance(st_endpoint(sr_geom),st_geomfromtext('POINT($end_lon $end_lat)', 4326)) < 0.001) OR (ST_Distance(st_endpoint(sr_geom),st_geomfromtext('POINT($start_lon $start_lat)', 4326)) < 0.001 AND ST_Distance(st_startpoint(sr_geom),st_geomfromtext('POINT($end_lon $end_lat)', 4326)) < 0.001)";
		
		#This as of 03/17/13
		#$routequery = "SELECT st_astext(st_line_interpolate_point(sr_geom, $pct_along_route)), id, feature_data FROM sr_layer_static_data where layer_id=2002 ORDER BY least(ST_Distance(st_startpoint(sr_geom),st_geomfromtext('POINT($start_lon $start_lat)',4326)) + ST_Distance(st_endpoint(sr_geom),st_geomfromtext('POINT($end_lon $end_lat)',4326)), ST_Distance(st_endpoint(sr_geom),st_geomfromtext('POINT($start_lon $start_lat)',4326)) + ST_Distance(st_startpoint(sr_geom),st_geomfromtext('POINT($end_lon $end_lat)',4326)) )  LIMIT 1";

		$routequery = "SELECT st_astext(st_line_interpolate_point(CASE WHEN (ST_line_locate_point(geometry, st_geomfromtext('POINT($start_lon $start_lat )', 4326))) < (ST_line_locate_point(geometry, st_geomfromtext('POINT($end_lon $end_lat )', 4326))) THEN (ST_line_substring(geometry, ST_line_locate_point(geometry, st_geomfromtext('POINT($start_lon $start_lat )', 4326)), ST_line_locate_point(geometry, st_geomfromtext('POINT($end_lon $end_lat )', 4326)))) ELSE (ST_line_substring(geometry, ST_line_locate_point(geometry, st_geomfromtext('POINT($end_lon $end_lat )', 4326)), ST_line_locate_point(geometry, st_geomfromtext('POINT($start_lon $start_lat )', 4326)))) END, 0.5) ), id, data FROM srmap where group_id=2015 AND data @> '\"SUBWAY\"=>\"$subwayline\"' AND (ST_Distance(geometry, st_geomfromtext('POINT($end_lon $end_lat )', 4326) ))= (select min(ST_Distance(geometry, st_geomfromtext('POINT($end_lon $end_lat )', 4326) )) from srmap where group_id=2015 AND data @> '\"SUBWAY\"=>\"$subwayline\"')";
		#ST_Distance(geometry, st_geomfromtext('POINT($end_lon $end_lat)', 4326)), data
		print "$routequery\n";

		$routequery_handle = $srdb->prepare($routequery);

		# EXECUTE THE QUERY
		$routequery_handle->execute();

		# BIND TABLE COLUMNS TO VARIABLES
		$routequery_handle->bind_columns(undef, \$train, \$dataid, \$routedata );

		$routequery_handle->fetch();

	
		#print "$tripid $dataid $pct_along_route $startid $endid		$subwayline  GEOM=$train\n";

		#update the sr tables


		$insertlocations = "insert into location (source, has_data, data ,geometry) values(6, 't', hstore(ARRAY[['type','train'], ['tripid','$tripid'], ['subwayline','$subwayline']]), St_Force_3D(St_GeomFromText( '$train', 4326) )   ) returning id";
		#print "$insertlocations\n";
		$insertlocations_handle = $srdb->prepare($insertlocations);
		# EXECUTE THE QUERY
		$insertlocations_handle->execute();
		#get the returned id
		$locid = $insertlocations_handle->fetch()->[0];


		### BEGIN CHECK ENTITY TO SEE IF IT ALREADY HAS A last_location_status_id and if so set the data_end value to now.
		#$query = "select id, last_location_status_id from entity where name='$tripid'";
		$lastentity_id=$entity_id;
		
		$query2 = "select e.id, es.location, es.id, degrees(st_azimuth(St_Force_3D(St_GeomFromText( '$train', 4326) ) , St_Force_3D(st_geomfromtext('POINT(' || st_x(geometry) || ' ' || st_y(geometry) || ')', 4326)))), st_distance(St_Force_3D(St_GeomFromText( '$train', 4326) ) , St_Force_3D(st_geomfromtext('POINT(' || st_x(geometry) || ' ' || st_y(geometry) || ')', 4326)))  from entity e, location l, entity_status es where es.data @> '\"trip_id\"=>\"$tripid\"'::hstore AND location is not null AND l.id=location AND e.id=es.entity AND es.has_end='f' order by es.updated desc";
		
		#print "$query2 ;\n";
		#Perhaps I could grab it's current location as well in order to calculate the direction it's moving

		$query2_handle = $srdb->prepare($query2);

		# EXECUTE THE QUERY
		$query2_handle->execute();
		#print "query2handle is $query2_handle\n";

		# BIND TABLE COLUMNS TO VARIABLES
		$query2_handle->bind_columns(\$entity_id, \$last_loc_id, \$last_loc_status_id, \$heading, \$distancetraveled );
		
		#This query could be null if the trip has just started... Deal with that
		my $found = $query2_handle->fetch();

		#print "entity_id is $entity_id \n";
		
		
		if($distancetraveled > 0.01){
			print "Distance traveled is $distancetraveled. $entity_id $tripid ... $dataid $routedata\n";
			print "$routequery\n";
		}	
		#or check if the entity_id is same as the last one
	
		$counter=$counter+1;
		#print "counter is at $counter\n";
	
		#print "found is $found\n";
		if ($found eq '') {
			
  			#Then it's a new trip and does not yet have a train assigned.
  			#Find a free train and give it this trip id
  			$tripquery = "SELECT e.id, es.id FROM entity e, entity_status es WHERE es.data @> '\"inservice\"=>\"false\"'::hstore AND has_end='f' and es.entity=e.id limit 1";
  			print "$tripquery\n";
			$tripquery_handle = $srdb->prepare($tripquery);
			$tripquery_handle->execute();
			$tripquery_handle->bind_columns(undef, \$entity_id, \$notinservicestatus );
			$tripquery_handle->fetch();
			 print "#######											Starting new trip with entity $entity_id ########################\n";
			 #select name from entity, entity_status where entity.status=entity_status.id AND entity_status.data @> '"inservice"=>"f"'::hstore AND has_end='f';
			 #find a train where the inservice status is f and has_end is false
			 
		 	#$insertlocations = "insert into location (source, has_data, data ,geometry) values(6, 't', hstore(ARRAY[['type','train'], ['tripid','$tripid'], ['subwayline','$subwayline']]), St_Force_3D(St_GeomFromText( 'POINT($oldx $oldy)', 4326) )   ) returning id";
			#$insertlocations_handle = $srdb->prepare($insertlocations);
			# EXECUTE THE QUERY
			#$insertlocations_handle->execute();
			#get the returned id
			#$startlocid = $insertlocations_handle->fetch()->[0];
			 
			 #doesn't have location data
			#$query = "insert into entity_status (entity, has_data, has_begin, location, data, data_begin) values ($entity_id, 'TRUE', 'TRUE', $startlocid, hstore(ARRAY[['subwayline','$subwayline'],['inservice','t'], ['trip_id','$tripid']]), now() ) returning id";

##HERE IT IS iNSERTING SERVICE
### SHOULD NOT HAVE TO INSERT has_XXX anymore- automatically done by trigger
			$insertquery = "insert into entity_status (entity,  data, data_begin) values ($entity_id, hstore(ARRAY[['subwayline','$subwayline'],['inservice','true'], ['trip_id','$tripid']]), now() ) returning id";
			$insertquery_handle = $srdb->prepare($insertquery);
			$insertquery_handle->execute();
			$insertquery_handle->bind_columns(undef, \$newentitystatusid );
			my $testfound = $insertquery_handle->fetch();
			if ($testfound eq '') {
				print "There's a problem with $entity_id on trip $tripid \n";
			}
			
			#print "$notinservicestatus is the notinservicestatus\n";
			
			#my $rows = $srdb->do("UPDATE entity set data = data || '\"tripid\"=>\"$tripid\"'::hstore WHERE id=$entity_id" );
			#close the inservice=f status and create an inservice=t with trip_id
			
			my $rows = $srdb->do("UPDATE entity_status set data_end=now() WHERE id=$notinservicestatus" );
  			#UPDATE hstore_test SET data = data || '"key4"=>"some value"'::hstore
  			
  			#set oldx and oldy to be the start geometry
		}


		#print "need to update entity_status,  last_location_status_id = $last_loc_status_id \n";

		### END CHECK EXISTING ENTITY VALUES

		#Calculate heading with 
		#This goes directly north. The second point is the start, and the first point is the end. ie How would I get from point A (1st point) starting at point B (second point).
		#$headingstatus = "select degrees(st_azimuth(St_Force_3D(St_GeomFromText( '$train', 4326) ) , st_geomfromtext('POINT($oldx $oldy)', 4326)))";
		#$headingstatus_handle = $srdb->prepare($headingstatus);
		#$headingstatus_handle->execute();

		#print "Entity ID is $entity_id\n";
		$insertstatus = "insert into entity_status (entity, has_data, has_begin, location, data, data_begin) values ($entity_id, 'TRUE', 'TRUE', $locid, hstore(ARRAY[['subwayline','$subwayline'], ['trip_id','$tripid'], ['heading','$heading'], ['routeid','$dataid']]), now() ) returning id";
		#Should include heading based on azimuth calculation ['heading','']
		$insertstatus_handle = $srdb->prepare($insertstatus);
		$insertstatus_handle->execute();
		#get the returned status id
		$statusid = $insertstatus_handle->fetch()->[0];

		### BEGIN UPDATE entity_status set data_end to now(). - the old status
		if($last_loc_status_id != '') {
			
			my $rows = $srdb->do("UPDATE entity_status set data_end=now(), has_end='TRUE' WHERE id=$last_loc_status_id" );
		}
		### END UPDATE entity_status
		


		#$statusid = $insertstatus_handle->fetch()->[0];


	} 
	#}

	#Select all the trains that ended their trips since the last time we updated, and close them out
	#search for trips that ended in the last $timeinterval til now (or maybe a few seconds buffer),
	#which don't have end times set
	
	#calculate the time interval we want to search (double it to be safe)
	($s1,$m1,$h1,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
	$newtime = sprintf ("%02d%02d%02d", $h1, $m1, $s1);		
	
	#Search for 5 minutes into the past to make sure everything gets closed
	($year2, $month2, $day2, $h2, $m2, $s2) = Add_Delta_DHMS( 1900, 02, 23, $h1, $m1, $s1, 0, 0, -5, 0);
	$searchtime = sprintf ("%02d%02d%02d", $h2, $m2, $s2);
	
	#$endquery = "select a.trip_id, x.stop_lon, x.stop_lat, a.arrival_time from (select arrival_time, trip_id, stop_sequence, stop_id from nyc_subways) a, (select stop_lat, stop_lon, stop_id from nyc_subway_stops) x, 
	#entity e, entity_status es where a.arrival_time >= '$searchtime' AND a.arrival_time < '$newtime' AND a.stop_sequence = (SELECT max(stop_sequence) FROM nyc_subways where trip_id=a.trip_id)
	#AND x.stop_id=a.stop_id AND e.last_location_status_id=es.id AND es.has_end='FALSE' AND e.name=a.trip_id";


	#	#SELECT city FROM weather WHERE temp_lo = (SELECT max(temp_lo) FROM weather);
	#$endquery = "select a.trip_id, x.stop_lon, x.stop_lat, a.arrival_time from (select arrival_time, trip_id, stop_sequence, stop_id from nyc_subways) a, (select stop_lat, stop_lon, stop_id from nyc_subway_stops) x where a.arrival_time >= '$searchtime' AND a.arrival_time < '$newtime' AND a.stop_sequence = (SELECT max(stop_sequence) FROM nyc_subways where trip_id=a.trip_id)
	#AND x.stop_id=a.stop_id";

	
	$endquery = "select a.trip_id, a.arrival_time from (select arrival_time, trip_id, stop_sequence, stop_id from nyc_subways) a, (select stop_lat, stop_lon, stop_id from nyc_subway_stops) x where a.arrival_time >= '$searchtime' AND a.arrival_time < '$newtime' AND a.stop_sequence = (SELECT max(stop_sequence) FROM nyc_subways where trip_id=a.trip_id)
	AND x.stop_id=a.stop_id AND a.trip_id ~ '$daysearch'";
	#print "$endquery\n";
	
#maybe don't do this all at once
#				s.layer_id=2002 AND s.feature_data LIKE '%\"ROUTE\":\"%$subwayline%\",\"NAME\"%' 
#				AND ST_Distance(st_endpoint(s.sr_geom),st_geomfromtext('POINT(x.stop_lon x.stop_lat)', 4326)) < 0.001) 
#OR ST_Distance(st_startpoint(s.sr_geom),st_geomfromtext('POINT(x.stop_lon x.stop_lat)', 4326)) < 0.001)

	#print "Endquery is $endquery \n";
	
	$endquery_handle = $dbh->prepare($endquery);

	# EXECUTE THE QUERY
	#I need to figure out how to end the trip, because it may require queries across databases ... or that's how it is now anyway.
	$endquery_handle->execute();

	# BIND TABLE COLUMNS TO VARIABLES
	#$endquery_handle->bind_columns(undef, \$endtripid, \$stoplon, \$stoplat, \$finishtime);
	$endquery_handle->bind_columns(undef, \$endtripid, \$finishtime);


	# LOOP THROUGH RESULTS
	while($endquery_handle->fetch()) {
		#Update the last location update, and set it to end
		my $rows = $srdb->do("UPDATE entity_status set data_end=now() WHERE data @> '\"trip_id\"=>\"$endtripid\"'::hstore AND data ? 'heading' AND has_end='f'" );
		#Update the inservice true to end
		$updateendquery = "UPDATE entity_status set data_end=now() WHERE data @> '\"trip_id\"=>\"$endtripid\"'::hstore AND data @> '\"inservice\"=>\"true\"'::hstore AND has_end='f' returning entity";
	#	print "Update is: $updateendquery\n";
		$updateendquery_handle = $srdb->prepare($updateendquery);
		$updateendquery_handle->execute();
		$updateendquery_handle->bind_columns(undef, \$entityidtoend );
		my $found = $updateendquery_handle->fetch();
		#get the returned entity id
		#$entityidtoend = $updateendquery_handle->fetch()->[0];
		
		if ($found eq ''){
			print "Entity for $endtripid NOT FOUND!!\n";
		} else {
					print "The trip has finished: $endtripid at $finishtime\n";
					my $rows = $srdb->do("insert into entity_status (entity, data) VALUES ($entityidtoend, hstore(ARRAY[['inservice','false']]))");
		}

		#set the true service status to end, and insert a new one that is false (thus freeing up the train for future trips)
		#create a new location based on the end, and set an end to the old one (and leave the new one open??)
	}

	#Note: 3/5/13
	#It now checks when a trip has ended, but it doesn't update the tables yet


}#end big while loop



# clean up
#$dbh->disconnect();


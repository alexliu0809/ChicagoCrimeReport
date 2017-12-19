#!/usr/bin/perl -w
# Creates an html table of flight delays by weather for the given route

# Needed includes
use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;
use Data::Dumper;

# Read the origin and destination airports as CGI parameters
my $community = param('community');
my $year = param('year');
my $month = param('month');

# Define a connection template to access the HBase REST server
# If you are on out cluster, hadoop-m will resolve to our Hadoop master
# node, which is running the HBase REST server
# my $hbase = HBase::JSONRest->new(host => "localhost:8080");
my $hbase = HBase::JSONRest->new(host => "10.0.0.5:8082");

# This function takes a row and gives you the value of the given column
# E.g., cellValue($row, 'delay:rain_delay') gives the value of the
# rain_delay column in the delay family.
# It uses somewhat tricky perl, so you can treat it as a black box
sub cellValue {
    my $row = $_[0];
    my $field_name = $_[1];
    my $row_cells = ${$row}{'columns'};
    foreach my $cell (@$row_cells) {
    if ($$cell{'name'} eq $field_name) {
        return $$cell{'value'};
    }
    }
    return 'missing';
}

# Query hbase for the route. For example, if the departure airport is ORD
# and the arrival airport is DEN, the "where" clause of the query will
# require the key to equal ORDDEN
my $batch_records = $hbase->get({
  table => 'crime_hbase_alexliu',
  where => {
      #key_equals => "Uptown-2004-01"
      #key_equals => "Auburn Gresham-2017-11"
      key_equals => $community.'-'.$year.'-'.$month
  },
});

my $speed_records = $hbase->get({
  table => 'speed_crime_alexliu',
  where => {
    #key_equals => "Uptown-2004-01"
    #key_equals => "Auburn Gresham-2017-11"
    key_equals => $community.'-'.$year.'-'.$month
  },
});

sub combinedCellValue {
    my $field_name = $_[0];
    if(!@$speed_records && !@$batch_records) {
      return "missing";
    }
    my $result = 0;
    if(@$batch_records) {
  $result += cellValue(@$batch_records[0], $field_name);
    }
    if(@$speed_records) {
  my $packed_value = cellValue(@$speed_records[0], $field_name);
  if($packed_value ne "missing") {
      $result += cellValue(@$speed_records[0], $field_name);
  }
    }
    return $result;
}

# There will only be one record for this route, which will be the
# "zeroth" row returned
my $batch_row = @$batch_records[0];
my $speed_row = @$speed_records[0];

# Get the value of all the columns we need and store them in named variables
# Perl's ability to assign a list of values all at once is very convenient here
my($Homecide_count, $Assault_count, $Robbery_count, $Battery_count, $Theft_count, $Burglary_count,
   $Prostitution_count, $Gambling_count, $Narcotics_count, $Kidnapping_count, $Other_Crime_count)
 =  (combinedCellValue('crime:Homecide_count'),
     combinedCellValue('crime:Assault_count'),
     combinedCellValue('crime:Robbery_count'),
     combinedCellValue('crime:Battery_count'),
     combinedCellValue('crime:Theft_count'),
     combinedCellValue('crime:Burglary_count'),
     combinedCellValue('crime:Prostitution_count'),
     combinedCellValue('crime:Gambling_count'),
     combinedCellValue('crime:Narcotics_count'),
     combinedCellValue('crime:Kidnapping_count'),
     combinedCellValue('crime:Other_Crime_count'));

# Given the number of flights and the total delay, this gives the average delay
sub average_delay {
    my($flights, $delay) = @_;
    return $flights > 0 ? sprintf("%.1f", $delay/$flights) : "-";
}

# Print an HTML page with the table. Perl CGI has commands for all the
# common HTML tags
print header, start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'/table.css',-type=>'text/css'}));
print div({-style=>'margin-left:275px;margin-right:auto;display:inline-block;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Crime Count For: ' . $community . ' in Year:' . $year . ' Month:' . $month . ' by crime type&nbsp;');
print     p({-style=>"bottom-margin:10px"});
print table({-class=>'CSS_Table_Example', -style=>'width:60%;margin:auto;'},
        Tr([td(['Homecide_count', 'Assault_count', 'Robbery_count', 'Battery_count', 'Theft_count', 'Burglary_count', 'Prostitution_count', 'Gambling_count', 'Narcotics_count', 'Kidnapping_count','Other_Crime_count']),
                td([$Homecide_count, $Assault_count, $Robbery_count, $Battery_count, $Theft_count, $Burglary_count, $Prostitution_count, $Gambling_count, $Narcotics_count, $Kidnapping_count, $Other_Crime_count])
        ])),
    p({-style=>"bottom-margin:10px"})
    ;

print end_html;

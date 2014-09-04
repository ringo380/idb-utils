#!/usr/bin/perl

#use diagnostics;
#use strict;
use warnings;

use DBI;
use DBD::mysql;
use Data::Dumper;
use bytes;

use Fcntl qw(:seek);
use Getopt::Long qw(:config gnu_getopt);
use Scalar::Util qw(looks_like_number);
use List::MoreUtils qw( each_array );

sub blank {$_ = '' for @_}

my ( $dbh, $un, $pw );

my ( $space_id, $table_id );

my ( $fh, $filename, $hex, $buffer, $offset, $page_size, $page_count,
    $file_size );
my ( $checksum, $spaceid, $pagenum, $lsn, $ptype, $oldchecksum, $low32lsn,
    $destfile );

my $mycnf_path  = "/root/.my.cnf";
my $mydata_path = "/var/lib/mysql";
my $debug       = 0;

my $help     = '';
my $file     = '';
my $ibdata   = '';
my $lsn_sync = '';
my $db       = '';
my $tbl      = '';
my $one_byte = '';
my $set_page = '';

GetOptions(
    'h'   => \$help,
    'f=s' => \$file,
    'i'   => \$ibdata,
    'l'   => \$lsn_sync,
    'b=i'   => \$one_byte,
    'd=s' => \$db,
    't=s' => \$tbl
) or die "Could not get options.\n";

$buffer = '';

if ($file) {
    $filename = $file;
    if ( !-e $filename ) {
        print STDERR "File path provided does not exist.\n";
        exit;
    }
    if ( !-r $filename ) {
        print STDERR "File path provided is not readable.\n";
        exit;
    }
}

$offset    = 0;
$page_size = 16384;

sub getbytes {
    local ( $byte_pos, $byte_count, $byte_file );
    ( $byte_pos, $byte_count, $byte_file ) = @_;
    $byte_file //= "ibdata1";
	$buffer = '';
    $filename = "$mydata_path/$byte_file";
    open( $fh, "<", $filename ) || die "Can't open $filename: $!";
    binmode($fh) || die "Can't binmode $filename: $!";

    sysseek( $fh, $byte_pos, SEEK_SET )
      || die "Couldn't seek to byte $byte_pos in $filename";
    sysread( $fh, $buffer, $byte_count )
      || die "Could not read to byte $byte_pos in $filename";
    $hex = unpack "H*", $buffer;
    close($fh);
    return hex $hex;
}

sub getbytes_ascii {
    local ( $byte_pos, $byte_count, $byte_file );
    ( $byte_pos, $byte_count, $byte_file ) = @_;
    $byte_file //= "ibdata1";
	$buffer = '';
    $filename = "$mydata_path/$byte_file";
    open( $fh, "<", $filename ) || die "Can't open $filename: $!";
    binmode($fh) || die "Can't binmode $filename: $!";

    sysseek( $fh, $byte_pos, SEEK_SET ) or die "Couldn't seek to byte $byte_pos in $filename";
    sysread( $fh, $buffer, $byte_count ) or die "Could not read to byte $byte_pos in $filename";
	
	$ascii = unpack "A*", $buffer;
	return $ascii;

}

sub writebytes {

    my ( $byte_pos, $byte_count, $byte_new, $byte_file );
    ( $byte_pos, $byte_count, $byte_new, $byte_file ) = @_;
    $filename = "$mydata_path/$byte_file";
    $buffer   = '';
    open FILE, "+<$filename" or die "Can't open $filename: $!";
    binmode FILE;

    # my @arr_data;
    # for ( my $idx = 0 ; $idx < 256 ; $idx++ ) {
    #     $arr_data[$idx] = $idx;
    # }

    seek (FILE, $byte_pos, SEEK_SET) or die "Couldn't seek to byte $byte_pos in $filename: $!\n";
    print FILE $byte_new or die "Couldn't write $byte_new to $filename at $byte_pos: $!\n";
}

sub getpage {

    local $page = $_[0];
    $page //= 0;
    local @attr;

    $offset = $page_size * $page;

    # Get Header
    push @attr, &getbytes( ( $offset + 4 ), 4 );    # 0 page number
    push @attr, &getbytes( $offset, 4 );            # 1 checksum
    push @attr, &getbytes( ( $offset + 34 ), 4 );   # 2 space id
    push @attr, &getbytes( ( $offset + 20 ), 4 );   # 3 lsn
    push @attr, &getbytes( ( $offset + 24 ), 2 );   # 4 page type

    # Get Trailer
    push @attr, &getbytes( ( $offset + 16376 ), 4 );    # 5 old-style checksum
    push @attr, &getbytes( ( $offset + 16380 ), 4 );    # 6 low 32 bits of lsn
    push @attr, $offset;    # 7 add the offset in there
    return @attr;
}

sub parse_config_file {

    local ( $config_line, $Name, $Value, $Config );

    ( $File, $Config ) = @_;

    if ( !open( CONFIG, "$File" ) ) {
        print "ERROR: Config file not found : $File";
        exit(0);
    }

    while (<CONFIG>) {
        $config_line = $_;

        #if ($config_line =~ /^[^\[client\]]*$/) {
        chop($config_line);          # Get rid of the trailling \n
        $config_line =~ s/^\s*//;    # Remove spaces at the start of the line
        $config_line =~ s/\s*$//;    # Remove spaces at the end of the line
        if (   ( $config_line !~ /^#/ )
            && ( $config_line ne "" )
            && ( $config_line !~ /^\[client\]*$/ ) )
        {    # Ignore lines starting with # and blank lines
            ( $Name, $Value ) = split( /=/, $config_line )
              ;    # Split each line into name value pairs
            $$Config{$Name} = $Value;    # Create a hash of the name value pairs
        }
    }

    close(CONFIG);

}

# Retrieve header values from ibdata1
my $idb_checksum	= &getbytes( 0,	4);
my $idb_offset		= &getbytes( 4, 4);
my $idb_prev_page	= &getbytes( 8, 4);
my $idb_next_page	= &getbytes( 12, 4);
my $idb_lastmod_lsn = &getbytes( 16, 8);
my $idb_page_type	= &getbytes( 24, 2);
my $idb_flush_lsn	= &getbytes( 26, 8);
my $idb_space_id	= &getbytes( 34, 4);



# Retrieve LSNs from ibdata1 and ib_logfile0
my $ibd_lsn   		= &getbytes( 20,   4 );
my $ibd_lsn2  		= &getbytes( 30,   4 );

my $log0_lsn  		= &getbytes( 524,  4, "ib_logfile0" );
my $log0_lsn2 		= &getbytes( 1548, 4, "ib_logfile0" );

# Call the subroutine
&parse_config_file( $mycnf_path, \%Config );

my $mypw     = $Config{password};
my $myun     = $Config{user};
my $host     = "localhost";
my $platform = "mysql";
my $port     = "3306";

$mypw =~ s/^"(.+)"$/$1/;
$schema = "information_schema";

#print "$mypw\n";
#print "$myun\n";

#$db = $ARGV[0];
#$tbl = $ARGV[1];



sub get_space_id {
    local ( $sql, $schema_table );
    $schema_table = "innodb_sys_tables";
    $sql = "select SPACE from $schema.$schema_table where NAME = \"$db/$tbl\"";
    if ($debug) { print "Space ID Query: $sql\n"; }
    $dbh->selectrow_array($sql);
}

sub get_table_id {
    local ( $sql, $schema_table );
    $schema_table = "innodb_sys_tables";
    $sql =
      "select TABLE_ID from $schema.$schema_table where NAME = \"$db/$tbl\"";
    if ($debug) { print "Table ID Query: $sql\n"; }
    $dbh->selectrow_array($sql);
}

# BEGIN ONE-SHOT OPTIONS

if ($ibdata) {

    #my $ibd_lsn = &getbytes( 30, 4 );
    #my $log0_lsn2 = &getbytes( 1548, 4, "ib_logfile0" );
    #my $log0_lsn = &getbytes( 524, 4, "ib_logfile0" );
    #my @page_items = &getpage(0);
	print "Primary Checksum from ibdata1 (Offset 0, len 4): $idb_checksum\n";
	print "Initial Offset / Page Number from ibdata1 (Offset 4, len 4): $idb_offset\n";
	print "Page Type: $idb_page_type\n";
    print "Initial LSN (offset 14) from ibdata1: $ibd_lsn\n";
    print "Primary LSN (offset 1E) from ibdata1: $ibd_lsn2\n";
    print "Primary LSN (offset 20C) from ib_logfile0: $log0_lsn\n";
    print "Current LSN (offset 60C) from ib_logfile0: $log0_lsn2\n";
    print "Flush LSN: $idb_flush_lsn\n";
    print "Last Modification LSN: $idb_lastmod_lsn\n";
    exit;
}

if ($one_byte) {
	print "Writing single byte integer ($one_byte) to file..\n";
	my $byte_file = "onebyte.fil";
	open FILEB, '>:', $byte_file or die "open failed: $!\n";
	# binmode FILEB;
	my $packed_byte = pack ('i>', $one_byte);
	print "Unpacked packed byte: ";
	print unpack('i', $packed_byte);
	print "\n";
	print FILEB $packed_byte;
	exit;
}

if ($lsn_sync) {
    print "Primary LSN in ibdata1: $ibd_lsn2\n";
    print "Primary LSN in ib_logfile0: $log0_lsn\n";
    if ( $ibd_lsn2 != $log0_lsn ) {
        print "LSNs out of sync. Attempting to sync now...\n";
        my $bindata = pack( 'i>', $log0_lsn );
        print "Hex Input: $bindata";

        #print unpack("I*", $bindata);
        print "\n";
        &writebytes( 30, 4, $bindata, "ibdata1" );
    }
    else {
        print "LSN already in sync.\n";
    }
    print "Primary LSN in ibdata1: $ibd_lsn2\n";
    print "Primary LSN in ib_logfile0: $log0_lsn\n";
    exit;
}

# Set the data source name information
$dsn = "dbi:$platform:$schema:$host:$port";
$dbh = DBI->connect( $dsn, $myun, $mypw ) or die "Could not connect to database: $DBI::errstr";

if ($debug) { print "MySQL connection to $host established successfully.\n"; }

# END ONE-SHOT OPTIONS

# Prepare Query
# $query = "select SPACE from $schema.innodb_sys_tables where NAME = \"$db/$tbl\"";
# $query_handle = $dbh->prepare($query)

sub get_indexes {
    local ( $sql, $schema_table, @names, @pages );
    $schema_table = "innodb_sys_indexes";
    $sql = "select NAME from $schema.$schema_table where TABLE_ID = $table_id";
    if ($debug) { print "Index Name Retrieval Query: $sql\n"; }

    @names = map { $_->[0] } @{ $dbh->selectall_arrayref($sql) };

    $sql =
      "select PAGE_NO from $schema.$schema_table where TABLE_ID = $table_id";
    if ($debug) { print "Index Page Retrieval Query: $sql\n"; }

    @pages = map { $_->[0] } @{ $dbh->selectall_arrayref($sql) };

    return ( \@names, \@pages );

}

sub get_inno_status {
    local ( $sql, @raw );
    $sql = "SHOW ENGINE INNODB STATUS";
    @raw = @{ $dbh->selectall_arrayref($sql) }
      or die "Could not execute query: $sql - $!\n";
    return @raw;
}

$space_id = &get_space_id or die "Could not retrieve $db.$tbl: $!";
$table_id = &get_table_id;

my @input      = &get_inno_status;
my $idb_status = $input[0][2];

my ($lsn_global) = $idb_status =~ m#Log sequence number.(\d+)#;
my ($lsn_flush)  = $idb_status =~ m#Log flushed up to\s+(\d+)#;
my ($txn_count)  = $idb_status =~ m#Trx id counter\s+(\d+)#;

my ( $idx_names, $idx_pages ) = &get_indexes;
my $indexes = each_array( @$idx_names, @$idx_pages );

print "\nInnoDB General Information\n";
print "------------------\n";
print "Log Sequence Number: $lsn_global\n";
print "Log Flushed?: ";
if ( $lsn_flush == $lsn_global ) { print "Yes. Flush point matches LSN.\n"; }
else { print "No. Flush point does not match LSN: $lsn_flush\n"; }
print "Transaction ID Counter: $txn_count\n";
print "\nInnoDB Table Information\n";
print "------------------\n";
print "Table: $db.$tbl\n";
print "Table ID: $table_id\n";
print "Space ID: $space_id\n";

print "\nINDEXES\n";
print "------------------\n";

while ( my ( $name, $page ) = $indexes->() ) {
    print "Index: $name - Page: $page\n";
}

# Execute
# $query_handle->execute();

# Get Space ID;
#$query_handle->bind_columns(\$space_id);

#while($query_handle->fetch()) {
#   print "$db/$tbl tablespace ID: $space_id\n";
#}

#print "Disconnecting..\n";
#$dbh->disconnect();

#!/usr/bin/perl -w
use strict;
use warnings;

use Test::More;
use Test::Exception;
use DBIx::ResultSet::Connector;

dies_ok { DBIx::ResultSet::Connector->new() } 'new without args dies';

my @args = ('dbi:SQLite:dbname=t/test.db', '', '');

my %test_cases = (
    connector => \@args,
    coerced   => [ dbix_connector => \@args ],
    verbose   => [ dbix_connector => DBIx::Connector->new( @args ) ],
);

foreach my $case (keys %test_cases) {
    my $case_args = $test_cases{$case};
    my $connector = DBIx::ResultSet::Connector->new( @$case_args );
    isa_ok( $connector->dbix_connector(), 'DBIx::Connector', $case . ' constructor:' );
}

done_testing;

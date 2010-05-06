package DBIx::ResultSet;
BEGIN {
  $DBIx::ResultSet::VERSION = '0.10';
}
use Moose;
use namespace::autoclean;

=head1 NAME

DBIx::ResultSet - Lightweight SQL query building and execution.

=head1 SYNOPSIS

    my $connector = DBIx::ResultSet::Connector->new( $dsn, $user, $pass );
    
    my $users = $connector->resultset('users');
    my $adult_users = $users->search({ age => {'>=', 18} });
    
    print 'Users: ' . $users->count() . "\n";
    print 'Adult users: ' . $adult_users->count() . "\n";

=head1 DESCRIPTION

This module provides an API that simpliefies the creation and execution
of SQL queries.  This is done by providing a thin wrapper around the
L<SQL::Abstract>, L<DBIx::Connector>, L<DBI>, L<Data::Page>, and
DateTime::Format::* modules.

I was inspired to write this module because I work in an environment
where we really didn't want the heavy footprint of L<DBIx::Class>,
but instead wanted many of the features of L<DBIx::Class::ResultSet>
in a lightweight package.

Unlike DBIx::Class, this module DOES expect you to be retrieving
thousands and millions of rows.  It is designed for high-volume
and optimized software, where the developers believe that writing
effecient code and elegant code is not mutually exclusive.

=cut

use Clone qw( clone );
use List::MoreUtils qw( uniq );
use Carp qw( croak );
use Data::Page;

=head1 METHODS

=head2 search

    my $old_rs = $connector->resultset('users')->search({ status => 0 });
    my $new_rs = $old_rs->search({ age > 18 });
    print 'Disabled adults: ' . $new_rs->count() . "\n";

Returns a new result set object that overlays the passed in where clause
on top of the old where clause, creating a new result set.  The original
result set's where clause is left unmodified.

=cut

sub search {
    my ($self, $where, $clauses) = @_;

    $where ||= {};
    my $new_where = clone( $self->where() );
    map { $new_where->{$_} = $where->{$_} } keys %$where;

    my $new_clauses = {};
    foreach my $clause (uniq sort (keys %$clauses, keys %{$self->clauses()})) {
        if (exists $clauses->{$clause}) {
            $new_clauses->{$clause} = clone( $clauses->{$clause} );
        }
        else {
            $new_clauses->{$clause} = clone( $self->clauses->{$clause} );
        }
    }

    return ref($self)->new(
        connector => $self->connector(),
        table     => $self->table(),
        where     => $new_where,
        clauses   => $new_clauses,
    );
}

sub _dbi_execute {
    my ($self, $dbh_method, $sql, $bind, $dbh_attrs) = @_;

    return $self->connector->run(sub{
        my ($dbh) = @_;
        my $sth = $dbh->prepare_cached( $sql );
        if ($dbh_method eq 'do') {
            $sth->execute( @$bind );
        }
        else {
            return $dbh->$dbh_method( $sth, $dbh_attrs, @$bind );
        }
        return;
    });
}

sub _dbi_prepare {
    my ($self, $sql) = @_;

    return $self->connector->run(sub{
        my ($dbh) = @_;
        return $dbh->prepare_cached( $sql );
    });
}

sub _do_select {
    my ($self, $fields) = @_;

    my $clauses = $self->clauses();

    if ($self->clauses->{page}) {
        $clauses->{limit}  = $self->pager->entries_per_page();
        $clauses->{offset} = $self->pager->skipped();
    }

    return $self->abstract->select(
        $self->table(), $fields, $self->where(),
        $clauses->{order_by},
        $clauses->{limit},
        $clauses->{offset},
    );
}

=head1 METHODS

=head2 insert

    $rs->insert(
        { user_name=>'bob2003', email=>'bob@example.com' }, # fields to insert
    );

=cut

sub insert {
    my ($self, $fields) = @_;
    my ($sql, @bind) = $self->abstract->insert( $self->table(), $fields );
    $self->_dbi_execute( 'do', $sql, \@bind );
    return;
}

=head2 update

    $rs->update(
        { phone => '555-1234' }, # fields to update
    );

=cut

sub update {
    my ($self, $fields) = @_;
    my ($sql, @bind) = $self->abstract->update( $self->table(), $fields, $self->where() );
    $self->_dbi_execute( 'do', $sql, \@bind );
    return;
}

=head2 delete

    $rs->delete();

=cut

sub delete {
    my ($self) = @_;
    my ($sql, @bind) = $self->abstract->delete( $self->table(), $self->where() );
    $self->_dbi_execute( 'do', $sql, \@bind );
    return;
}

=head2 array_row

    my $user = $rs->array_row(
        ['user_id', 'created', 'email', 'phone'], # fields to retrieve
    );

=cut

sub array_row {
    my ($self, $fields) = @_;
    my ($sql, @bind) = $self->_do_select( $fields );
    return [ $self->_dbi_execute( 'selectrow_array', $sql, \@bind ) ];
}

=head2 hash_row

    my $user = $rs->hash_row(
        ['user_id', 'created'],     # fields to retrieve
    );

=cut

sub hash_row {
    my ($self, $fields) = @_;
    my ($sql, @bind) = $self->_do_select( $fields );
    return $self->_dbi_execute( 'selectrow_hashref', $sql, \@bind );
}

=head2 array_of_array_rows

    my $disabled_users = $rs->array_of_array_rows(
        ['user_id', 'email', 'phone'], # fields to retrieve
    );
    print $disabled_users->[2]->[1];

Returns an array ref of array refs, one for each row returned.

=cut

sub array_of_array_rows {
    my ($self, $fields) = @_;
    my ($sql, @bind) = $self->_do_select( $fields );
    return $self->_dbi_execute( 'selectall_arrayref', $sql, \@bind );
}

=head2 array_of_hash_rows

    my $disabled_users = $rs->array_of_hash_rows(
        ['user_id', 'email', 'phone'], # fields to retrieve
    );
    print $disabled_users->[2]->{email};

=cut

sub array_of_hash_rows {
    my ($self, $fields) = @_;
    my ($sql, @bind) = $self->_do_select( $fields );
    return $self->_dbi_execute( 'selectall_arrayref', $sql, \@bind, { Slice=>{} } );
}

=head2 hash_of_hash_rows

    my $disabled_users = $rs->hash_of_hash_rows(
        'user_name',                   # column to index the hash by
        ['user_id', 'email', 'phone'], # fields to retrieve
    );
    print $disabled_users->{jsmith}->{email};

=cut

sub hash_of_hash_rows {
    my ($self, $key, $fields) = @_;
    my ($sql, @bind) = $self->_do_select( $fields );
    return $self->connector->run(sub{
        my ($dbh) = @_;
        my $sth = $dbh->prepare_cached( $sql );
        return $dbh->selectall_hashref( $sth, $key, {}, @bind );
    });
}

=head2 count

    my $enabled_users_count = $rs->count();

=cut

sub count {
    my ($self) = @_;
    return $self->pager->entries_on_this_page() if $self->clauses->{page};
    my ($sql, @bind) = $self->_do_select( 'COUNT(*)' );
    return ( $self->_dbi_execute( 'selectrow_array', $sql, \@bind ) )[0];
}

=head2 column

    my $user_ids = $rs->column(
        'user_id',                          # column to retrieve
    );

=cut

sub column {
    my ($self, $column) = @_;
    my ($sql, @bind) = $self->_do_select( $column );
    return $self->_dbi_execute( 'selectcol_arrayref', $sql, \@bind );
}

=head2 select_sth

    my ($sth, @bind) = $rs->select_sth(
        ['user_name', 'user_id'], # fields to retrieve
    );
    $sth->execute( @bind );
    $sth->bind_columns( \my( $user_name, $user_id ) );
    while ($sth->fetch()) { ... }

If you want a little more power, or want you DB access a little more
effecient for your particular situation, then you might want to get
at the select sth.

=cut

sub select_sth {
    my ($self, $fields) = @_;
    my ($sql, @bind) = $self->_do_select( $fields );
    return( $self->_dbi_prepare( $sql ), @bind );
}

=head2 insert_sth

    my $insert_sth;
    foreach my $user_name (qw( jsmith bthompson gfillman )) {
        my $fields = {
            user_name => $user_name,
            email     => $user_name . '@mycompany.com',
        };

        $insert_sth ||= $rs->insert_sth(
            $fields, # fields to insert
        );

        $insert_sth->execute(
            $rs->bind_values( $fields ),
        );
    }

If you're going to insert a *lot* of records you probably don't want to
be re-generating the SQL every time you call insert().

=cut

sub insert_sth {
    my ($self, $fields) = @_;
    my ($sql, @bind) = $self->abstract->insert( $fields );
    return $self->_dbi_prepare( $sql );
}

=head2 bind_values

This mehtod is a non-modifying wrapper around L<SQL::Abstract>'s values()
method to be used in conjunction with insert_sth().

=cut

sub bind_values {
    my ($self, $fields) = @_;
    return $self->abstract->values( $fields );
}

=head1 ATTRIBUTES

=head2 connector

=cut

has 'connector' => (
    is       => 'ro',
    isa      => 'DBIx::ResultSet::Connector',
    required => 1,
    handles => [qw(
        dbh
        run
        txn
        svp
        abstract
    )],
);

=head2 pager

    my $rs = $connector->resultset('users')->search({}, {page=>2, rows=>50});
    my $pager = $rs->pager(); # a pre-populated Data::Page object

A L<Data::Page> object pre-populated based on page() and rows().  If
page() has not been specified then trying to access page() will throw
a fatal error.

The total_entries and last_page methods are proxied from the pager in
to this class so that you can call:

    print $rs->total_entries();

Instead of:

    print $rs->pager->total_entries();

=cut

has 'pager' => (
    is         => 'ro',
    isa        => 'Data::Page',
    lazy_build => 1,
    handles => [qw(
        total_entries
        last_page
    )],
);
sub _build_pager {
    my ($self) = @_;

    croak 'pager() can only be called on pageing result sets' if !$self->clauses->{page};

    my $pager = Data::Page->new();
    $pager->total_entries( $self->search({}, {page=>0})->count() );
    $pager->entries_per_page( $self->clauses->{rows} || 10 );
    $pager->current_page( $self->clauses->{page} );

    return $pager;
}

=head2 table

The name of the table that this result set will be using for queries.

=cut

has 'table' => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

=head2 where

The where clause hash ref to be used when executing queries.

=cut

has 'where' => (
    is      => 'ro',
    isa     => 'HashRef',
    default => sub{ {} },
);

=head2 clauses

Additional clauses, such as order_by, limit, offset, etc.

=cut

has 'clauses' => (
    is      => 'ro',
    isa     => 'HashRef',
    default => sub{ {} },
);

__PACKAGE__->meta->make_immutable;
1;

use v5.22.0;
package Test::PgMonger;
# ABSTRACT: a thing for managing Postgres databases, for testing

use Moose;

use experimental qw(postderef signatures);

use DBI;

use namespace::autoclean;

package Test::PgMonger::TempDB {

  use Moose;
  use experimental qw(postderef signatures);
  use namespace::autoclean;

  has dsn      => (is => 'ro', required => 1);
  has dbname   => (is => 'ro', required => 1);
  has username => (is => 'ro', required => 1);
  has password => (is => 'ro', required => 1);
  has _pid     => (is => 'ro', default  => sub { $$ });

  has monster  => (is => 'ro', required => 1);

  has _is_dead => (is => 'rw', default  => 0);

  sub connect_info ($self) {
    return (
      $self->dsn,
      $self->username,
      $self->password,
    );
  }

  sub cleanup ($self) {
    my $dbh = $self->monster->master_dbh;
    $dbh->do("DROP DATABASE " . $self->dbname);
    $dbh->do("DROP USER " . $self->username);
    $self->_is_dead(1);
  }

  sub DEMOLISH ($self, @) {
    return if $self->_is_dead;
    return if $self->_pid != $$;
    $self->cleanup;
  }
}

has dsn      => (is => 'ro', default => 'dbi:Pg:');
has username => (is => 'ro', default => 'postgres');
has password => (is => 'ro', default => undef);
has basename => (is => 'ro', default => 'test_pgmonger');
has template => (is => 'ro', default => 'PID_T_N');

sub master_dbh ($self) {
  return $self->_master_dbh unless $self->_has_master_dbh;
  return $self->_master_dbh if $self->_master_dbh->ping;
  $self->_clear_master_dbh;
  return $self->_master_dbh;
}

has _master_dbh => (
  is      => 'ro',
  isa     => 'Object',
  lazy    => 1,
  predicate => '_has_master_dbh',
  clearer   => '_clear_master_dbh',
  default   => sub ($self) {
    DBI->connect(
      $self->dsn,
      $self->username,
      $self->password,
      { RaiseError => 1 },
    );
  }
);

sub usernames ($self) {
  my $usernames = $self->master_dbh->selectcol_arrayref(
    'SELECT usename FROM pg_catalog.pg_user'
  );

  return grep { 0 == index $_, $self->basename } @$usernames;
}

sub databases ($self) {
  my $databases = $self->master_dbh->selectcol_arrayref(
    'SELECT datname FROM pg_catalog.pg_database'
  );

  return grep { 0 == index $_, $self->basename } @$databases;
}

my %EXPANDO = (PID => $$, T => $^T, N => sub { state $n; $n++ });

sub create_database ($self, $arg = {}) {
  state $n;
  $n++;

  my @hunks = split /_/, $self->template;
  @hunks = map {; ref $EXPANDO{$_} ? $EXPANDO{$_}->()
                :     $EXPANDO{$_} ? $EXPANDO{$_}
                :                    $_               } @hunks;

  my $name = join q{_}, $self->basename, @hunks;

  $self->master_dbh->do("CREATE USER $name WITH PASSWORD '$name'");

  $self->master_dbh->do("CREATE DATABASE $name WITH TEMPLATE template0 ENCODING 'UTF8' OWNER $name");

  my $tempdb_dsn = $self->dsn . "dbname=$name";

  if ($arg->{extra_sql_statements}) {
    my $master_tmp_dbh = DBI->connect(
      $tempdb_dsn,
      $self->username,
      $self->password,
      { RaiseError => 1 },
    );

    for my $stmt (@{ $arg->{extra_sql_statements} }) {
      $master_tmp_dbh->do(ref $stmt ? @$stmt : $stmt);
    }
  }

  return Test::PgMonger::TempDB->new({
    dsn      => $tempdb_dsn,
    dbname   => $name,
    username => $name,
    password => $name,
    monster  => $self,
  });
}

sub clean_house ($self) {
  my $master_dbh = $self->master_dbh;
  for my $database ($self->databases) {
    $master_dbh->do("DROP DATABASE $database");
  }

  for my $username ($self->usernames) {
    $master_dbh->do("DROP USER $username");
  }

  return;
}

1;

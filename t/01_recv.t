#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use Test::PostgreSQL;
use AnyEvent;
use File::Basename;
use File::Spec;
use Promises backend => ['AnyEvent'], qw(deferred);

use ok 'AnyEvent::PgRecvlogical';

my $t_dir = File::Spec->rel2abs(dirname(__FILE__));
my $pg_hba_conf = File::Spec->join($t_dir, 'pg_hba.conf');

my $pg = eval {
    Test::PostgreSQL->new(extra_postmaster_args =>
          "-c hba_file='$pg_hba_conf' -c wal_level=logical -c max_wal_senders=1 -c max_replication_slots=1");
}
  or plan skip_all => "cannot create test postgres database: $Test::PostgreSQL::errstr";

my $control = DBI->connect($pg->dsn, 'postgres');

my $end_cv = AE::cv;

my $recv = new_ok(
    'AnyEvent::PgRecvlogical' => [
        dbname         => 'test',
        host           => '127.0.0.1',
        port           => $pg->port,
        username       => 'postgres',
        slot           => 'test',
        options        => { 'skip-empty-xacts' => 1 },
        do_create_slot => 1,
        slot_exists_ok => 1,
        heartbeat      => 1,
        on_message     => sub { pass "got message: $_[0]"; $end_cv->send(1) if $_[0] =~ /payload\[text\]:'qwerty'/ },
        on_error => sub { fail $_[0]; $end_cv->croak(@_) },
    ],
    'pg_recvlogical'
);

ok $recv->dbh, 'connected';

$recv->start->done(
    sub {
        pass 'replication started';

        $control->do('create table test_tbl (id int primary key, payload text)');
        $control->do('insert into test_tbl (id, payload) values (?, ?)', undef, 1, 'qwerty');
    },
    sub {
        fail 'replication started';
        diag @_;
    }
);

# let some heartbeats go by
my $cv = AE::cv;
$cv->begin; my $wt = AE::timer 3, 0, sub { $cv->end };
$cv->recv;

ok $end_cv->recv, 'got a message';
$recv->stop;

done_testing;

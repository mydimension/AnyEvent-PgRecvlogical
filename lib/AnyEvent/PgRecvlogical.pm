package AnyEvent::PgRecvlogical;

# ABSTRACT: perl port of pg_recvlogical

use Moo;
use DBI;
use DBD::Pg 3.7.0 ':async';
use AnyEvent;
use AnyEvent::Util 'guard';
use Promises backend => ['AnyEvent'], qw(deferred);
use Types::Standard ':all';
use Try::Tiny;
use Carp 'croak';

use constant {
    AWAIT_INTERVAL    => 1,
    USECS             => 1_000_000,
    PG_MIN_VERSION    => 9_04_00,
    PG_MIN_NOEXPORT   => 10_00_00,
    PG_STATE_DUPEOBJ  => '42710',
    PG_EPOCH_DELTA    => 946_684_800,
    XLOGDATA          => 'Aq>3a*',
    PRIMARY_HEARTBEAT => 'Aq>2b',
    STANDBY_HEARTBEAT => 'Aq>4b',
};

use namespace::clean;

my $DBH = (InstanceOf ['DBI::db'])->create_child_type(
    constraint => sub {
        $_->{Driver}->{Name} eq 'Pg'
          and $_->{pg_server_version} >= PG_MIN_VERSION
          and $_->{Name} =~ /replication=/;
    },
    message => sub {
        my $parent_check = (InstanceOf['DBI::db'])->validate($_);
        return $parent_check if $parent_check;
        return "$_ is not a DBD::Pg handle" unless $_->{Driver}->{Name} eq 'Pg';
        return "$_ is connected to an old postgres version ($_->{pg_server_version} < 9.4.0)" unless $_->{pg_server_version} >= PG_MIN_VERSION;
        return "$_ is not a replication connection: $_->{Name}" unless $_->{Name} =~ /replication=/;
    }
);

my $LSNStr = Str->where(sub { m{[0-9A-F]{1,8}/[0-9A-F]{1,8}} })
  ->plus_coercions(Int() => sub { sprintf '%X/%X', (($_ >> 32) & 0xffff_ffff), ($_ & 0xffff_ffff) });

my $LSN = Int->plus_coercions(
    $LSNStr => sub {
        my ($h, $l) = map { hex } split m{/}; ($h << 32) | $l;
    }
);

has dbname   => (is => 'ro', isa => Str, required  => 1);
has host     => (is => 'ro', isa => Str, predicate => 1);
has port     => (is => 'ro', isa => Int, predicate => 1);
has username => (is => 'ro', isa => Str, default => q{});
has password => (is => 'ro', isa => Str, default => q{});
has slot     => (is => 'ro', isa => Str, required  => 1);

has dbh => (is => 'lazy', isa => $DBH, clearer => 1, init_arg => undef);
has do_create_slot     => (is => 'ro', isa => Bool, default   => 0);
has slot_exists_ok     => (is => 'ro', isa => Bool, default   => 0);
has reconnect          => (is => 'ro', isa => Bool, default   => 1);
has reconnect_delay    => (is => 'ro', isa => Int,  default   => 5);
has reconnect_limit    => (is => 'ro', isa => Int,  predicate => 1);
has _reconnect_counter => (is => 'rw', isa => Int,  default   => 0);
has heartbeat          => (is => 'ro', isa => Int,  default   => 10);
has plugin             => (is => 'ro', isa => Str,  default   => 'test_decoding');
has options => (is => 'ro', isa => HashRef, default => sub { {} });
has startpos     => (is => 'rwp', isa => $LSN, default => 0, coerce  => 1);
has received_lsn => (is => 'rwp', isa => $LSN, default => 0, clearer => 1, init_arg => undef, lazy => 1);
has flushed_lsn  => (is => 'rwp', isa => $LSN, default => 0, clearer => 1, init_arg => undef, lazy => 1);

has on_message => (is => 'ro', isa => CodeRef, required => 1);
has on_error => (is => 'ro', isa => CodeRef, default => sub { \&croak });

has _fh_watch => (is => 'lazy', isa => Ref, clearer => 1);
has _timer    => (is => 'lazy', isa => Ref, clearer => 1);

sub _dsn {
    my $self = shift;

    my %dsn = (replication => 'database', client_encoding => 'sql_ascii');
    foreach (qw(host port dbname)) {
        my $x = "has_$_";
        next if $self->can($x) and not $self->$x;

        $dsn{$_} = $self->$_;
    }

    return 'dbi:Pg:' . join q{;}, map { "$_=$dsn{$_}" } sort keys %dsn;
}

sub _build_dbh {
    my $self = shift;
    return DBI->connect(
        $self->_dsn,
        $self->username,
        $self->password,
        { PrintError => 0 },
    );
}

sub _build__fh_watch {
    my $self = shift;
    return AE::io $self->dbh->{pg_socket}, 0, sub { $self->_read_copydata };
}

sub _build__timer {
    my $self = shift;
    return AE::timer $self->heartbeat, $self->heartbeat, sub { $self->_heartbeat };
}

sub start {
    my $self = shift;

    $self->_post_init(
        deferred {
            shift->chain(sub { $self->identify_system }, sub { $self->create_slot }, sub { $self->start_replication });
        }
    );
}

sub _post_init {
    my ($self, $d) = @_;

    return $d->then(
        sub {
            $self->_fh_watch;
            $self->_timer;
        },
        $self->on_error,
    );
}

sub identify_system {
    my $self = shift;
    $self->dbh->do('IDENTIFY_SYSTEM', { pg_async => PG_ASYNC });
    return _async_await($self->dbh);
}

sub create_slot {
    my $self = shift;

    return deferred->resolve unless $self->do_create_slot;

    my $dbh = $self->dbh;
    $dbh->do(
        sprintf(
            'CREATE_REPLICATION_SLOT %s LOGICAL %s%s',
            $dbh->quote_identifier($self->slot),
            $dbh->quote_identifier($self->plugin),
            ($dbh->{pg_server_version} >= PG_MIN_NOEXPORT ? ' NOEXPORT_SNAPSHOT' : '')    # uncoverable branch true
        ),
        { pg_async => PG_ASYNC }
    );

    return _async_await($dbh)->catch(
        sub {
            croak @_ unless $dbh->state eq PG_STATE_DUPEOBJ and $self->slot_exists_ok;
        }
    );
}

sub _option_string {
    my $self = shift;

    my @opts;
    while (my ($k, $v) = each %{ $self->options }) {
        push @opts, $self->dbh->quote_identifier($k);
        defined $v and $opts[-1] .= sprintf ' %s', $self->dbh->quote($v);    # uncoverable branch false
    }

    return @opts ? sprintf('(%s)', join q{, }, @opts) : q{};    # uncoverable branch false
}

sub start_replication {
    my $self = shift;

    $self->dbh->do(
        sprintf(
            'START_REPLICATION SLOT %s LOGICAL %s%s',
            $self->dbh->quote_identifier($self->slot),
            $LSNStr->coerce($self->startpos),
            $self->_option_string
        ),
    );
}

sub _read_copydata {
    my $self = shift;

    my ($n, $msg);
    my $ok = try {
        $n = $self->dbh->pg_getcopydata_async($msg);
        1;
    }
    catch {
        # uncoverable statement count:2
        AE::postpone { $self->_handle_disconnect };
        0;
    };

    # exception thrown, going to reconnect
    return unless $ok;    # uncoverable branch true

    # nothing waiting
    return if $n == 0;

    if ($n == -1) {
        AE::postpone { $self->_handle_disconnect };
        return;
    }

    # uncoverable branch true
    if ($n == -2) {
        # error reading
        # uncoverable statement
        $self->on_error->('could not read COPY data: ' . $self->dbh->errstr);
    }

    my $type = substr $msg, 0, 1;

    if ('k' eq $type) {
        # server keepalive
        my (undef, $lsnpos, $ts, $reply) = unpack PRIMARY_HEARTBEAT, $msg;

        # only interested in the request-reply bit
        # uncoverable branch true
        if ($reply) {
            # uncoverable statement
            AE::postpone { $self->_heartbeat };
        }

        # an inbound heartbeat is proof enough of successful reconnect
        $self->_reconnect_counter(0) if $self->_reconnect_counter;

        return;
    }

    # uncoverable branch true
    unless ('w' eq $type) {
        # uncoverable statement
        $self->on_error->("unrecognized streaming header: '$type'");
    }

    my (undef, $startlsn, $endlsn, $ts, $record) = unpack XLOGDATA, $msg;

    $self->_set_received_lsn($startlsn);

    my $guard = guard {
        $self->_set_flushed_lsn($startlsn) if $startlsn > $self->flushed_lsn;
    };

    $self->on_message->($record, $guard);

    # do it again until $n == 0
    my $w; $w = AE::timer 0, 0, sub { undef $w; $self->_read_copydata };
}

sub stop {
    my $self = shift;

    $self->_clear_fh_watch;
    $self->_clear_timer;
    $self->clear_dbh;
}

sub _handle_disconnect {
    my $self = shift;

    $self->stop;

    return unless $self->reconnect;

    if (    $self->has_reconnect_limit
        and $self->_reconnect_counter($self->_reconnect_counter + 1) > $self->reconnect_limit) {
        $self->on_error->('reconnect limit reached: ' . $self->reconnect_limit);
        return;
    }

    $self->_set_startpos($self->flushed_lsn);
    $self->clear_received_lsn;
    $self->clear_flushed_lsn;

    my $w; $w = AE::timer $self->reconnect_delay, 0, sub {
        undef $w;
        $self->_post_init(deferred { $self->start_replication });
    };
}

sub _heartbeat {
    my ($self, $req_reply) = @_;
    $req_reply = !!$req_reply || 0;    #uncoverable condition right

    my $status = pack STANDBY_HEARTBEAT, 'r',     # receiver status update
      $self->received_lsn,                        # last WAL received
      $self->flushed_lsn,                         # last WAL flushed
      0,                                          # last WAL applied
      int((AE::now - PG_EPOCH_DELTA) * USECS),    # ms since 2000-01-01
      $req_reply;                                 # request heartbeat

    $self->dbh->pg_putcopydata($status);
}

sub _async_await {
    my ($dbh) = @_;

    my $d = deferred;

    # no async operation in progress
    return $d->reject if $dbh->{pg_async_status} == 0;    # uncoverable branch true

    my $w; $w = AE::timer 0, AWAIT_INTERVAL, sub {
        return unless $dbh->pg_ready;
        try {
            $d->resolve($dbh->pg_result);
        }
        catch {
            $d->reject($_);
        };
        undef $w;
    };

    return $d->promise;
}

1;

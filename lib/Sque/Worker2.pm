package Sque::Worker;
use Any::Moose;
use Any::Moose '::Util::TypeConstraints';
with 'Sque::Encoder';

use POSIX ":sys_wait_h";
use Sys::Hostname;
use Scalar::Util qw(blessed weaken);
use List::MoreUtils qw{ uniq any };
use DateTime;
use Try::Tiny;

# ABSTRACT: Does the hard work of babysitting Sque::Job's

use overload
    '""' => \&_string,
    '==' => \&_is_equal,
    'eq' => \&_is_equal;

=attr sque

The L<Sque> object running this worker.

=cut
has sque => (
    is => 'ro',
    required => 1,
    handles => [qw/ stomp key /]
);

=attr queues

Queues this worker should fetch jobs from.

=cut
has queues => (
    is => 'rw',
    isa => 'ArrayRef',
    lazy => 1,
    default => sub {[]}
);

=attr id

Unique identifier for the running worker.
Used to set process status all around.

The worker stringify to this attribute.

=cut
has id => ( is => 'rw', lazy => 1, default => sub { $_[0]->_stringify } );
sub _string { $_[0]->id } # can't point overload to a mo[o|u]se attribute :-(

=attr verbose

Set to a true value to make this worker report what's doing while
on work().

=cut
has verbose => ( is => 'rw', default => sub {0} );

=attr cant_fork

Set it to a true value to stop this worker from fork jobs.

By default, the worker will fork the job out and control the
children process. This make the worker more resilient to
memory leaks.

=cut
has cant_fork => ( is => 'rw', default => sub {0} );

=attr child

PID of current running child.

=cut
has child => ( is => 'rw' );

=attr shutdown

When true, this worker will shutdown after finishing current job.

=cut
has shutdown => ( is => 'rw', default => sub{0} );

=attr paused

When true, this worker won't proccess more jobs till false.

=cut
has paused => ( is => 'rw', default => sub{0} );
has interval => ( is => 'rw', default => sub{5} );

=method pause

Stop processing jobs after the current one has completed (if we're
currently running one).

=cut
sub pause { $_[0]->paused(1) }

=method unpause

Start processing jobs again after a pause

=cut
sub unpause { $_[0]->paused(0) }

=method shutdown_please

Schedule this worker for shutdown. Will finish processing the
current job.

=cut
sub shutdown_please {
    print "Shutting down...\n";
    $_[0]->shutdown(1);
}

=method shutdown_now

Kill the child and shutdown immediately.

=cut
sub shutdown_now { $_[0]->shutdown_please && $_[0]->kill_child }

=method work

Calling this method will make this worker to start pulling & running jobs
from queues().

This is the main wheel and will run while shutdown() is false.

=cut
sub work {
    my $self = shift;
    $self->startup;
    while ( ! $self->shutdown ) {
        if ( !$self->paused && ( my $job = $self->reserve ) ) {
            $self->log("Got job $job");
            $self->work_tick($job);
        }
        elsif( $self->interval ) {
            my $status = $self->paused ? "Paused" : 'Waiting for ' . join( ', ', @{$self->queues} );
            $self->procline( $status );
            $self->log( $status );
            sleep $self->interval;
        }
    }
}

=method work_tick

Perform() one job and wait till it finish.

=cut
sub work_tick {
    my ($self, $job) = @_;

    $self->working_on($job);
    my $timestamp = DateTime->now->strftime("%Y/%m/%d %H:%M:%S %Z");

    if ( !$self->cant_fork && ( my $pid = fork ) ) {
        $self->procline( "Forked $pid at $timestamp" );
        $self->child($pid);
        $self->log( "Waiting for $pid" );
        #while ( ! waitpid( $pid, WNOHANG ) ) { } # non-blocking has sense?
        waitpid( $pid, 0 );
        $self->log( "Forked job($pid) exited with status $?" );
    }
    else {
        $self->procline( sprintf( "Processing %s since %s", $job->queue, $timestamp ) );
        $self->perform($job);
        exit(0) unless $self->cant_fork;
    }

    $self->done_working;
    $self->child(0);
}


=method perform

Call perform() on the given Sque::Job capturing and reporting
any exception.

=cut
sub perform {
    my ( $self, $job ) = @_;
    my $ret;
    try {
        $ret = $job->perform;
        $self->log( sprintf( "done: %s", $job->stringify ) );
    }
    catch {
        $self->log( sprintf( "%s failed: %s", $job->stringify, $_ ) );
        #$job->fail($_);
    };
    $ret;
}

=method kill_child

Kills the forked child immediately, without remorse. The job it
is processing will not be completed.

=cut
sub kill_child {
    my $self = shift;
    return unless $self->child;
    if ( kill 0, $self->child ) {
        $self->log( "Killing my child: " . $self->child );
        kill 9, $self->child;
    }
    else {
        $self->log( "Child " . $self->child . " not found, shutting down." );
        $self->shutdown_please;
    }
}

=method add_queue

Add a queue this worker should listen to.

=cut
sub add_queue {
    my $self = shift;
    return unless @_;
    my $queue = $self->sque->key( @_ );
    $self->queues( [ uniq( @{$self->queues}, $queue ) ] );
    $self->_subscribe_queue( $queue );
}

=method del_queue

Stop listening to the given queue.

=cut
sub del_queue {
    my ( $self, $queue ) = @_;
    return unless $queue;
    
    return
    @{$self->queues}
           -
    @{$self->queues( [ grep {$_} map { $_ eq $queue ? undef : $_ } @{$self->queues} ] )};
}


=method next_queue

Circular iterator over queues().

=cut
sub next_queue {
    my $self = shift;
    if ( @{$self->queues} > 1 ) {
        push @{$self->queues}, shift @{$self->queues};
    }
    return $self->queues->[-1];
}

=method reserve

Pull the next job to be precessed.

=cut
sub reserve {
    my $self = shift;
    return $self->sque->pop;
}

=method working_on

Set worker and working status on the given L<Sque::Job>.

=cut
sub working_on {
    my ( $self, $job ) = @_;
    $job->worker($self);
}

=method done_working

Inform the backend this worker has done its current job

=cut
sub done_working {
    my $self = shift;
    #$self->processed(1);
    # TODO: Ack stomp message
}

sub _stringify {
    my $self = shift;
    join ':', hostname, $$, join( ',', @{$self->queues} );
}

# Is this worker the same as another worker?
sub _is_equal {
    my ($self, $other) = @_;
    $self->id eq $other->id;
}

=method procline

Given a string, sets the procline ($0) and logs.
Procline is always in the format of:
sque-VERSION: STRING

=cut
sub procline {
    my $self = shift;
    if ( my $str = shift ) {
        $0 = sprintf( "sque-%s: %s", $Resque::VERSION || 'devel', $str );
    }
    $0;
}

=method startup

Helper method called by work() to:
1. register_signal_handlers()

=cut
sub startup {
    my $self = shift;
    $0 = 'sque: Starting';

    $self->register_signal_handlers;
}

=method register_signal_handlers

Registers the various signal handlers a worker responds to.
TERM: Shutdown immediately, stop processing jobs.
INT: Shutdown immediately, stop processing jobs.
QUIT: Shutdown after the current job has finished processing.
USR1: Kill the forked child immediately, continue processing jobs.
USR2: Don't process any new jobs
CONT: Start processing jobs again after a USR2

=cut
sub register_signal_handlers {
    my $self = shift;
    weaken $self;
    $SIG{TERM} = sub { $self->shutdown_now };
    $SIG{INT} = sub { $self->shutdown_now };
    $SIG{QUIT} = sub { $self->shutdown_please };
    $SIG{USR1} = sub { $self->kill_child };
    $SIG{USR2} = sub { $self->pause };
    $SIG{CONT} = sub { $self->unpause };
}

=method worker_pids

Returns an Array of string pids of all the other workers on this
machine. Useful when pruning dead workers on startup.

=cut
sub worker_pids {
    my $self = shift;
    my @pids;
    for ( split "\n", `ps axo pid,command | grep sque` ) {
        if ( m/^\s*(\d+)\s(.+)$/ ) {
            push @pids, $1;
        }
    }
    return wantarray ? @pids : \@pids;
}

=method log

If verbose() is true, this will print to STDERR.

=cut
#TODO: add logger() attr to containg a logger object and if set, use that instead of print!
sub log {
    my $self = shift;
    return unless $self->verbose;
    print STDERR shift, "\n";
}

sub _subscribe_queue {
    my $self = shift;
    $self->stomp->subscribe(
        destination => @_,
        ack         => 'client',
    );
};

__PACKAGE__->meta->make_immutable();

1;

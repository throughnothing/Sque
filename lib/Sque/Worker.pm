package Sque::Worker;
use Any::Moose;
use Any::Moose '::Util::TypeConstraints';
use Try::Tiny;

with 'Sque::Encoder';

# ABSTRACT: Does the hard work of babysitting Sque::Job's

has sque => (
    is => 'ro',
    required => 1,
    handles => [qw/ stomp key /]
);

has queues => (
    is => 'rw',
    isa => 'HashRef',
    lazy => 1,
    default => sub {{}}
);

has verbose => ( is => 'rw', default => sub {0} );

sub work {
    my ( $self ) = @_;
    while( my $job = $self->sque->pop ) {
        $job->worker($self);
        my $reval = $self->perform($job);
        #TODO: re-send messages to queue... ABORT messages?
        # if(!$reval){ }
    }
}

sub perform {
    my ( $self, $job ) = @_;
    my $ret;
    try {
        $job->perform;
        $self->log( sprintf( "done: %s", $job->stringify ) );
        $ret = 1;
    }
    catch {
        $self->log( sprintf( "%s failed: %s", $job->stringify, $_ ) );
        # Increment the Job retries and send the job back on the queue
        $job->retries( $job->retries + 1 );
        $self->sque->push( $job );
        $ret = 0;
    };
    # Ack original frame. If perform failed, we've sent it back on the queue
    $self->stomp->ack({ frame => $job->frame });
    return $ret;
}

sub reserve {
    my ( $self ) = @_;
    return $self->sque->pop;
}

sub add_queues {
    my $self = shift;
    return unless @_;
    for my $q ( @_ ) {
        if(!$self->queues->{$q}){
            $self->queues->{$q} = 1;
            my $queue = $self->sque->key( $q );
            $self->_subscribe_queue( $queue );
        }
    }
    $self;
}

sub log {
    my $self = shift;
    return unless $self->verbose;
    print STDERR shift, "\n";
}

sub _subscribe_queue {
    my ( $self, $q ) = @_;
    $self->stomp->subscribe( {
        destination => $q,
        ack         => 'client',
    } );
};

__PACKAGE__->meta->make_immutable();

1;

=attr sque

The L<Sque> object running this worker.

=attr queues

Queues this worker should fetch jobs from.

=attr verbose

Set to a true value to make this worker report what's doing while
on work().

=method work

Calling this method will make this worker to start pulling & running jobs
from queues().

This is the main wheel and will run while shutdown() is false.

=method perform

Call perform() on the given Sque::Job capturing and reporting
any exception.

=method reserve

Call reserve() to return the next job popped of the queue(s)

=method add_queues

Add a queue this worker should listen to.

=method log

If verbose() is true, this will print to STDERR.


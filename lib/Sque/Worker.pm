package Sque::Worker;
use Any::Moose;
use Any::Moose '::Util::TypeConstraints';
with 'Sque::Encoder';

use Try::Tiny;

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
    my $self = shift;
    while( my $job = $self->sque->pop ) {
        $job->worker($self);
        my $reval = $self->perform($job);
        if(!$reval){
            #TODO: re-send messages to queue... ABORT messages?
        }
        $self->stomp->ack( frame => $job->frame );
    }
}

sub perform {
    my ( $self, $job ) = @_;
    my $ret;
    try {
        $ret = $job->perform;
        $self->log( sprintf( "done: %s", $job->stringify ) );
    }
    catch {
        $self->log( sprintf( "%s failed: %s", $job->stringify, $_ ) );
        # TODO send to failed queue ?
    };
    $ret;
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
}

sub log {
    my $self = shift;
    return unless $self->verbose;
    print STDERR shift, "\n";
}

sub _subscribe_queue {
    my ( $self, $q ) = @_;
    $self->stomp->subscribe(
        destination => $q,
        id          => $q,
        ack         => 'client',
    );
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

=method add_queues

Add a queue this worker should listen to.

=method log

If verbose() is true, this will print to STDERR.


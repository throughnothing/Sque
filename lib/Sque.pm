use strict;
use warnings;
package Sque;
use Any::Moose;
use Any::Moose '::Util::TypeConstraints';
use Net::STOMP::Client;

# ABSTRACT: Background job processing based on Resque, using Stomp

use Sque::Job;
use Sque::Worker;

=attr stomp

A Stomp Client on this sque instance.

=cut
subtype 'Sugar::Stomp' => as class_type('Net::STOMP::Client');

coerce 'Sugar::Stomp'
    => from 'Str'
    => via {
        my ( $host, $port ) = split /:/;
        my $stomp = Net::STOMP::Client->new( host => $host, port => $port );
        $stomp->connect();
        return $stomp;
    };

has stomp => (
    is => 'ro',
    lazy => 1,
    coerce => 1,
    isa => 'Sugar::Stomp',
    default => sub { Net::STOMP::Client->new->connect },
);

=attr namespace

Namespace for queues, default is 'sque'

=cut
has namespace => ( is => 'rw', default => sub { 'sque' });

=attr worker

A L<Sque::Worker> on this sque instance.

=cut
has worker => (
    is => 'ro',
    lazy => 1,
    default => sub { Sque::Worker->new( sque => $_[0] ) },
);

=method push

Pushes a job onto a queue. Queue name should be a string and the
item should be a Sque::Job object or a hashref containing:
class - The String name of the job class to run.
args - Any arrayref of arguments to pass the job.

Example
$sque->push( archive => { class => 'Archive', args => [ 35, 'tar' ] } )

=cut
sub push {
    my ( $self, $queue, $job ) = @_;
    confess "Can't push an empty job." unless $job;
    $job = $self->new_job($job) unless ref $job eq 'Sque::Job';
    $self->stomp->send(
        persistent => 'true',
        destination => $self->key( $queue ),
        body => $job->encode,
    );
}

=method pop

Pops a job off a queue. Queue name should be a string.
Returns a Sque::Job object.

=cut
sub pop {
    my ( $self ) = @_;
    my $frame = $self->stomp->receive_frame;
    return unless $frame;

    $self->new_job({
        frame => $frame,
        queue => $frame->header('destination'),
    });
}

=method new_job

Build a Sque::Job object on this system for the given
hashref(see Sque::Job) or string(payload for object).

=cut
sub new_job {
    my ( $self, $job ) = @_;

    if ( $job && ref $job && ref $job eq 'HASH' ) {
         return Sque::Job->new({ sque => $self, %$job });
    }
    elsif ( $job ) {
        return Sque::Job->new({ sque => $self, payload => $job });
    }
    confess "Can't build an empty Sque::Job object.";
}

=head1 HELPER METHODS

=method key

Concatenate $self->namespace with the received array of names
to build a redis key name for this sque instance.

=cut
sub key {
    my $self = shift;
    '/queue/' . $self->namespace . '/' . shift;
}


__PACKAGE__->meta->make_immutable();

1;

package Sque::Job;
use Any::Moose;
use Any::Moose '::Util::TypeConstraints';
use UNIVERSAL::require;
with 'Sque::Encoder';

# ABSTRACT: Sque job container

=attr sque
=cut
has sque => (
    is => 'rw',
    handles => [qw/ stomp /],
    default => sub { confess "This Sque::Job isn't associated to any Sque system yet!" }
);

=attr worker

Worker running this job.
A new worker will be popped up from sque by default.

=cut
has worker => (
    is => 'rw',
    lazy => 1,
    default => sub { $_[0]->sque->worker },
    predicate => 'has_worker'
);

=attr class

Class to be performed by this job.

=cut
has class => (
    is => 'rw',
    lazy => 1,
    default => sub { confess "This job needs a class to do some work." },
);

=attr queue

Name of the queue this job is or should be.

=cut
has queue => (
    is => 'rw', lazy => 1,
    default => \&queue_from_class,
    predicate => 'queued'
);

=attr args

Array of arguments for this job.

=cut
has args => ( is => 'rw', isa => 'ArrayRef', default => sub {[]} );

=attr payload

HashRef representation of the job.
When passed to constructor, this will restore the job from encoded state.
When passed as a string this will be coerced using JSON decoder.
This is read-only.

=cut
coerce 'HashRef'
    => from 'Str'
    => via { JSON->new->utf8->decode($_) };

has payload => (
    is => 'rw',
    isa => 'HashRef',
    coerce => 1,
    lazy => 1,
    default => sub {{
        class => $_[0]->class,
        args => $_[0]->args
    }},
    trigger => sub {
        my ( $self, $hr ) = @_;
        $self->class( $hr->{class} );
        $self->args( $hr->{args} ) if $hr->{args};
    }
);

=attr frame

Raw stomp frame representing the job.
This is read-only.

=cut
has frame => (
    is => 'ro',
    lazy => 1,
    default => sub { {} },
    trigger => sub {
        my ( $self, $frame ) = @_;
        $self->payload( $frame->body );
    }
);

=method encode

String representation(JSON) to be used on the backend.

=cut
sub encode {
    my $self = shift;
    $self->encoder->encode( $self->payload );
}

=method stringify
=cut
sub stringify {
    my $self = shift;
    sprintf( "(Job{%s} | %s | %s)",
        $self->queue,
        $self->class,
        $self->encoder->encode( $self->args )
    );
}

=method queue_from_class

Normalize class name to be used as queue name.
    NOTE: future versions will try to get the
    queue name from the real class attr
    or $class::queue global variable.

=cut
sub queue_from_class {
    my $class = shift->class;
    $class =~ s/://g;
    $class;
}

=method perform

Load job class and call perform() on it.
This job objet will be passed as the only argument.

=cut
sub perform {
    my $self = shift;
    $self->class->require || confess $@;
    $self->class->can('perform')
        || confess $self->class . " doesn't know how to perform";

    no strict 'refs';
    &{$self->class . '::perform'}($self);
}



__PACKAGE__->meta->make_immutable();

1;

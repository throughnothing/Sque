package Sque::Job;
use Any::Moose;
use Any::Moose '::Util::TypeConstraints';
use UNIVERSAL::require;
with 'Sque::Encoder';

# ABSTRACT: Sque job container

has sque => (
    is => 'rw',
    handles => [qw/ stomp /],
    default => sub { confess "This Sque::Job isn't associated to any Sque system yet!" }
);

has worker => (
    is => 'rw',
    lazy => 1,
    default => sub { $_[0]->sque->worker },
    predicate => 'has_worker'
);

has class => (
    is => 'rw',
    lazy => 1,
    default => sub { confess "This job needs a class to do some work." },
);

has queue => (
    is => 'rw', lazy => 1,
    default => \&queue_from_class,
    predicate => 'queued'
);

has args => ( is => 'rw', isa => 'ArrayRef', default => sub {[]} );

has retries => (
    is => 'rw',
    isa => 'Int',
    default => 0,
    trigger => sub {
        my ( $self, $retries ) = @_;
        # Set the retiers on the payload
        $self->payload->{retries} = $retries;
    }
);

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
        args => $_[0]->args,
        retries => $_[0]->retries,
    }},
    trigger => sub {
        my ( $self, $hr ) = @_;
        $self->class( $hr->{class} );
        $self->args( $hr->{args} ) if $hr->{args};
        $self->retries( $hr->{retries} ) if $hr->{retries};
    }
);

has headers => (
    is => 'rw',
    isa => 'HashRef',
    default => sub{ {} },
);

has frame => (
    is => 'ro',
    lazy => 1,
    default => sub { {} },
    trigger => sub {
        my ( $self, $frame ) = @_;
        $self->queue( $self->sque->unkey( $frame->destination ) );
        $self->payload( $frame->body );
    }
);

sub encode {
    my $self = shift;
    $self->encoder->encode( $self->payload );
}

sub stringify {
    my $self = shift;
    sprintf( "(Job{%s} | %s | %s)",
        $self->queue,
        $self->class,
        $self->encoder->encode( $self->args )
    );
}
sub queue_from_class {
    my $class = shift->class;
    $class =~ s/://g;
    $class;
}

sub perform {
    my $self = shift;
    $self->class->require || confess $@;

    # First test if its OO
    if($self->class->can('new')){
        no strict 'refs';
        $self->class->new->perform( $self );
    }else{
        # If it's not OO, just call perform
        $self->class->can('perform')
            || confess $self->class . " doesn't know how to perform";

        no strict 'refs';
        &{$self->class . '::perform'}($self);
    }
}

__PACKAGE__->meta->make_immutable();

1;

=attr sque

=attr worker

Worker running this job.
A new worker will be popped up from sque by default.

=attr class

Class to be performed by this job.

=attr queue

Name of the queue this job is or should be.

=attr args

Array of arguments for this job.

=attr payload

HashRef representation of the job.
When passed to constructor, this will restore the job from encoded state.
When passed as a string this will be coerced using JSON decoder.
This is read-only.

=attr frame

Raw stomp frame representing the job.
This is read-only.

=method encode

String representation(JSON) to be used on the backend.

=method stringify

=method queue_from_class

Normalize class name to be used as queue name.
    NOTE: future versions will try to get the
    queue name from the real class attr
    or $class::queue global variable.

=method perform

Load job class and call perform() on it.
This job objet will be passed as the only argument.

=cut

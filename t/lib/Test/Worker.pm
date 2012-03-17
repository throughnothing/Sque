package Test::Worker;
use v5.10.0;
#use Moose;
#has test => ( is => 'ro', default => 'test' );

sub perform {
    my ( $self, $job ) = @_;
    say $job->args->[0];
}

1;

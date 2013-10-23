package Test::WorkerMooseFail;
use Moose;

sub perform {
    my ( $self, $job ) = @_;
    die "I'm just gonna lay here and die :(";
}

1;

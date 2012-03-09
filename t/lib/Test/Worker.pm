package Test::Worker;
use v5.10.0;

sub perform {
    my $job = shift;
    say $job->args->[0];
}

1;

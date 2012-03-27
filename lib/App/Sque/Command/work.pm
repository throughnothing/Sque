package App::Sque::Command::work;
use App::Sque -command;
use Parallel::ForkManager;
use Sque;

# ABSTRACT: Worker command for sque command-line tool

sub usage_desc { "Start sque worker(s)" }

sub opt_spec {
    return (
        [ "host|h=s",  "Set the stomp host" ],
        [ "lib|l=s@",  "Add a lib directory if needed to find worker classes" ],
        [ "port|p=i",  "Set the stomp port" ],
        [ "queues|q=s",  "Comma-separted list of queues to listen to" ],
        [ "verbose|v",  "Be verbose?" ],
        [ "workers|w=i",  "Number of workers to start", { default => 1 } ],
    );
}

sub validate_args {
    my ($self, $opt, $args) = @_;

    # We must have the host/part
    $self->usage_error("host required") unless $opt->{host};
    $self->usage_error("port required") unless $opt->{port};

    # We must have queues
    $self->usage_error("queues required") unless $opt->{queues};
}

sub execute {
    my ($self, $opt, $args) = @_;

    # Use user-specified lib directories
    unshift @INC, @{ $opt->{lib} };
    my @queues = split /,/, $opt->{queues};

    if($opt->{verbose}){
        $" =", ";
        print "Listening to: @queues with $opt->{workers} worker(s).\n";
    }

    $pm = new Parallel::ForkManager($opt->{workers});
    while (1) {
        my $pid = $pm->start and next;

        my $w = Sque->new( stomp => "$opt->{host}:$opt->{port}" )->worker;
        $w->verbose( $opt->{verbose} ? 1 : 0 );
        $w->add_queues( @queues );
        $w->work;

        $pm->finish;
    }
}

1;

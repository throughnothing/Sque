package App::Sque::Command::send;
use App::Sque -command;
use Sque;

# ABSTRACT: Send command for sque command-line tool

sub usage_desc { "Send sque message." }

sub opt_spec {
    return (
        [ "class|c=s",  "Worker class to process message" ],
        [ "host|h=s",  "Set the stomp host" ],
        [ "port|p=i",  "Set the stomp port" ],
        [ "queue|q=s",  "Queue to send message to, defaults to worker class" ],
    );
}

sub validate_args {
    my ($self, $opt, $args) = @_;

    # We must have the host/part
    $self->usage_error("host required") unless $opt->{host};
    $self->usage_error("port required") unless $opt->{port};

    # We must have class
    $self->usage_error("class required") unless $opt->{class};

    $self->usage_error("at least on arg required") unless @$args > 0;
}

sub execute {
    my ($self, $opt, $args) = @_;

    if( ! defined $opt->{queue} ){
        $opt->{queue} = $opt->{class};
        $opt->{queue} =~ s/://g;
    }

    Sque->new( stomp => "$opt->{host}:$opt->{port}" )
        ->push( $opt->{queue} => { class => $opt->{class}, args => $args });
}

1;

use Test::More;
use lib 't/lib';
use Test::SpawnMq qw( mq );
use Sque;

my ( $s, $server ) = mq();

sub END { $s->() if $s }

my $sque = new_ok( "Sque" => [( stomp => $server )],
                    "Build Sque object $server" );

my $worker = $sque->worker;
$worker->add_queues( 'Test' );

# Push Moose Worker
push_job( $sque,
    class => 'Test::WorkerMooseFail',
    args => [ 'MOOSE', 'test' ]
);

# The job should have been pushed back to the queue
for( 0 .. 4 ) {
    my $job = $worker->reserve;
    is $job->retries => $_, 'Job back on queue with retries incremented';
    is $worker->perform( $job ) => 0, 'Job Failed again';
}

sub push_job {
    my ( $sque, %args ) = @_;
    $args{class} //= 'Test::Worker';
    $args{queue} //= 'Test';
    $args{args} //= [ 'DEFAULT', 'ARGS' ];

    ok( $sque->push( $args{queue} => {
            class => $args{class},
            args =>  $args{args}
        }), "Push new job to $args{queue} queue" );
}


done_testing;

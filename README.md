# NAME

Sque - Background job processing based on Resque, using Stomp

# VERSION

version 0.003

# SYNOPSIS

First you create a Sque instance where you configure the [Stomp](http://search.cpan.org/perldoc?Stomp)
backend and then you can start sending jobs to be done by workers:

    use Sque;

    my $s = Sque->new( stomp => '127.0.0.1:61613' );

    $s->push( my_queue => {
        class => 'My::Task',
        args => [ 'Hello world!' ]
    });

You can also send by just using:

    $s->push({
        class => 'My::Task',
        args => [ 'Hello world!' ]
    });

In this case, the queue will be set automatically automatically to the
job class name with colons replaced with hyphens, which in this
case would be 'My-Task'.

Background jobs can be any perl module that implement a perform() function.
The [Sque::Job](http://search.cpan.org/perldoc?Sque::Job) object is passed as the only argument to this function:

    package My::Task;
    use strict;
    use 5.10.0;

    sub perform {
        my ( $job ) = @_;
        say $job->args->[0];
    }

    1;

Background jobs can also be OO.  The perform function will still be called
with the [Sque::Job](http://search.cpan.org/perldoc?Sque::Job) object as the only argument:

    package My::Task;
    use strict;
    use 5.10.0;
    use Moose;

    with 'Role::Awesome';

    has attr => ( is => 'ro', default => 'Where am I?' );

    sub perform {
        my ( $self, $job ) = shift;
        say $self->attr;
        say $job->args->[0];
    }

    1;

Finally, you run your jobs by instancing a [Sque::Worker](http://search.cpan.org/perldoc?Sque::Worker) and telling it
to listen to one or more queues:

    use Sque;

    my $w = Sque->new( stomp => '127.0.0.1:61613' )->worker;
    $w->add_queues('my_queue');
    $w->work;

# DESCRIPTION

This is a copy of [resque-perl](https://github.com/diegok/resque-perl)
by [Diego Kuperman](https://github.com/diegok) simplified a little bit
(for better or worse) and made to work with any stomp server rather than Redis.

# ATTRIBUTES

## stomp

A Stomp Client on this sque instance.

## namespace

Namespace for queues, default is 'sque'

## worker

A [Sque::Worker](http://search.cpan.org/perldoc?Sque::Worker) on this sque instance.

# METHODS

## push

Pushes a job onto a queue. Queue name should be a string and the
item should be a [Sque::Job](http://search.cpan.org/perldoc?Sque::Job) object or a hashref containing:
class - The String name of the job class to run.
args - Any arrayref of arguments to pass the job.

Example:

    $sque->push( archive => { class => 'Archive', args => [ 35, 'tar' ] } )

## pop

Pops a job off a queue. Queue name should be a string.
Returns a l<Sque::Job> object.

## key

Concatenate `$self-`namespace> with the received array of names
to build a redis key name for this sque instance.

## new_job

Build a [Sque::Job](http://search.cpan.org/perldoc?Sque::Job) object on this system for the given
hashref(see [Sque::Job](http://search.cpan.org/perldoc?Sque::Job)) or string(payload for object).

# ATTRIBUTES

# HELPER METHODS

# AUTHOR

William Wolf <throughnothing@gmail.com>

# COPYRIGHT AND LICENSE



William Wolf has dedicated the work to the Commons by waiving all of his
or her rights to the work worldwide under copyright law and all related or
neighboring legal rights he or she had in the work, to the extent allowable by
law.

Works under CC0 do not require attribution. When citing the work, you should
not imply endorsement by the author.
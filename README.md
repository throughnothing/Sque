# NAME

Sque - Background job processing based on Resque, using Stomp

# VERSION

version 0.001

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
item should be a Sque::Job object or a hashref containing:
class - The String name of the job class to run.
args - Any arrayref of arguments to pass the job.

Example
$sque->push( archive => { class => 'Archive', args => [ 35, 'tar' ] } )

## pop

Pops a job off a queue. Queue name should be a string.
Returns a Sque::Job object.

## new_job

Build a Sque::Job object on this system for the given
hashref(see Sque::Job) or string(payload for object).

## key

Concatenate $self->namespace with the received array of names
to build a redis key name for this sque instance.

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

use strict;
use warnings;



use ExtUtils::MakeMaker 6.30;



my %WriteMakefileArgs = (
  "ABSTRACT" => "Background job processing based on Resque, using Stomp",
  "AUTHOR" => "William Wolf <throughnothing\@gmail.com>",
  "BUILD_REQUIRES" => {
    "Capture::Tiny" => 0,
    "Exporter" => 0,
    "Moose" => 0,
    "POE::Component::MessageQueue" => 0,
    "POSIX" => 0,
    "Test::More" => 0,
    "base" => 0
  },
  "CONFIGURE_REQUIRES" => {
    "ExtUtils::MakeMaker" => "6.30"
  },
  "DISTNAME" => "Sque",
  "EXE_FILES" => [
    "bin/sque"
  ],
  "LICENSE" => "unrestricted",
  "NAME" => "Sque",
  "PREREQ_PM" => {
    "Any::Moose" => 0,
    "App::Cmd::Setup" => 0,
    "JSON" => 0,
    "Net::Stomp" => 0,
    "Parallel::ForkManager" => 0,
    "Try::Tiny" => 0,
    "UNIVERSAL::require" => 0,
    "strict" => 0,
    "warnings" => 0
  },
  "VERSION" => "0.008",
  "test" => {
    "TESTS" => "t/*.t"
  }
);


unless ( eval { ExtUtils::MakeMaker->VERSION(6.56) } ) {
  my $br = delete $WriteMakefileArgs{BUILD_REQUIRES};
  my $pp = $WriteMakefileArgs{PREREQ_PM};
  for my $mod ( keys %$br ) {
    if ( exists $pp->{$mod} ) {
      $pp->{$mod} = $br->{$mod} if $br->{$mod} > $pp->{$mod};
    }
    else {
      $pp->{$mod} = $br->{$mod};
    }
  }
}

delete $WriteMakefileArgs{CONFIGURE_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.52) };

WriteMakefile(%WriteMakefileArgs);




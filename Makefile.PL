
use strict;
use warnings;



use ExtUtils::MakeMaker 6.31;



my %WriteMakefileArgs = (
                       'test' => {
                                   'TESTS' => 't/*.t'
                                 },
                       'NAME' => 'DBIx::ResultSet',
                       'DISTNAME' => 'DBIx-ResultSet',
                       'CONFIGURE_REQUIRES' => {
                                                 'ExtUtils::MakeMaker' => '6.31'
                                               },
                       'AUTHOR' => 'Aran Deltac <bluefeet@gmail.com>',
                       'BUILD_REQUIRES' => {
                                             'DateTime' => '0.55',
                                             'DateTime::Format::SQLite' => '0.11',
                                             'Test::Exception' => '0.29',
                                             'DBD::SQLite' => '1.29',
                                             'Test::Simple' => '0.94'
                                           },
                       'ABSTRACT' => 'Lightweight SQL query building and execution.',
                       'EXE_FILES' => [],
                       'VERSION' => '0.13',
                       'PREREQ_PM' => {
                                        'Data::Page' => '2.02',
                                        'SQL::Abstract' => '1.65',
                                        'SQL::Abstract::Limit' => '0.141',
                                        'namespace::autoclean' => '0.09',
                                        'MooseX::Types' => '0.21',
                                        'Module::Load' => '0.18',
                                        'DBIx::Connector' => '0.33',
                                        'Clone' => '0.31',
                                        'List::MoreUtils' => '0.22',
                                        'Moose' => '1.01'
                                      },
                       'LICENSE' => 'perl'
                     );


delete $WriteMakefileArgs{BUILD_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.56) };

delete $WriteMakefileArgs{CONFIGURE_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.52) };

WriteMakefile(%WriteMakefileArgs);




#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use lib 'lib';

use Lock::File;
use Yandex::X;
use PPB::Test::TFiles;

$ENV{LANG} = 'ru_RU.utf8';
if (!xfork) {
    my $lock = lockf("./tfiles/lock");
    sleep 2;
    exit(0);
} else {
    sleep 1;
    ok((not defined lockf("./tfiles/lock", {blocking => 0})), 'version 3 returns undef when already locked');
}

done_testing;

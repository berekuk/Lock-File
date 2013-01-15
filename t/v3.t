#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use lib 'lib';

use autodie qw(mkdir fork);

use File::Path qw(remove_tree);
remove_tree('tfiles');
mkdir 'tfiles';

{
    package t::v3;
    use Lock::File qw(lockf);
    use Test::More;

    if (!fork) {
        my $lock = lockf("./tfiles/lock");
        sleep 2;
        exit(0);
    } else {
        sleep 1;
        ok((not defined lockf("./tfiles/lock", {blocking => 0})), 'version 3 returns undef when already locked');
        ok((not defined lockf("./tfiles/lock", {timeout => 0})), 'version 3 returns undef when already locked and timeout=0 specified');
    }
    while(wait != -1) {}
}

done_testing;

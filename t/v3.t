#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use lib 'lib';
use PPB::Test::TFiles;

{
    package t::no_v;
    use Lock::File;
    use Yandex::X;
    use Test::Exception;

    if (!xfork) {
        my $lock = lockf("./tfiles/lock");
        sleep 2;
        exit(0);
    } else {
        sleep 1;
        throws_ok(sub {
            lockf("./tfiles/lock", {blocking => 0});
        }, qr/temporarily unavailable/, 'version 1 throws exception when already locked');
    }
    while(wait != -1) {}
}

{
    package t::v2;
    use Lock::File 2.0;
    use Yandex::X;
    use Test::Exception;

    if (!xfork) {
        my $lock = lockf("./tfiles/lock");
        sleep 2;
        exit(0);
    } else {
        sleep 1;
        throws_ok(sub {
            lockf("./tfiles/lock", {blocking => 0});
        }, qr/temporarily unavailable/, 'version 2 throws exception when already locked');
        throws_ok(sub {
            lockf("./tfiles/lock", {timeout => 0});
        }, qr/temporarily unavailable/, 'version 2 throws exception when already locked and timeout=0 specified');
    }
    while(wait != -1) {}
}

{
    package t::v3;
    use Lock::File 3.0;
    use Yandex::X;
    use Test::More;

    if (!xfork) {
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

{
    package t::no_v_again;
    use Lock::File;
    use Yandex::X;
    use Test::Exception;

    if (!xfork) {
        my $lock = lockf("./tfiles/lock");
        sleep 2;
        exit(0);
    } else {
        sleep 1;
        throws_ok(sub {
            lockf("./tfiles/lock", {blocking => 0});
        }, qr/temporarily unavailable/, 'no version again, throws exception when already locked');
    }
    while(wait != -1) {}
}

done_testing;

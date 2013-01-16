#!/usr/bin/perl

use strict;

use lib 'lib';
use Lock::File qw(:all);
use Time::HiRes qw(sleep);

use autodie qw(:all);
use IPC::System::Simple;

use Test::More;
use Test::Fatal;
use base qw(Test::Class);

use File::Path qw(remove_tree);

sub xqx {
    my $result = qx(@_);
    if ($?) {
        die "qx(@_) failed: $?";
    }
    return $result;
}

sub xprint {
    my $fh = shift;
    my $result = print {$fh} @_;
    die "Print failed: $!" unless $result;
    return;
}

sub setup :Test(setup) {
    remove_tree('tfiles');
    mkdir 'tfiles';
}

sub child_ok {
    my ($check, $msg) = @_;
    $check = $check ? 1 : 0;
    open my $fh, '>>', "tfiles/child.$$";
    xprint $fh, "$check $msg\n";
}

sub parent_ok {
    my @files = glob "tfiles/child.*";
    return unless @files;
    for (split /\n/, xqx("cat tfiles/child.*")) {
        /^([01]) (.*)/;
        ok($1, $2);
    }
}

sub wait_all {
    while () {
        my $pid = wait;
        last if $pid == -1;
        is($?, 0, "$pid exit");
    }
}

sub t($) {
    my ($time) = @_;
    my $sleep_period = $ENV{SLEEP_PERIOD} || 0.1;
    return $time * $sleep_period;
}
# sleep N tacts
sub tsleep($) {
    my ($time) = @_;
    sleep(t($time));
}

# forked_tests(sub { parent sub code }, sub { child sub code }, ...);
#
# This function does two things:
# 1) reduces boilerplate;
# 2) guarantees that child always exits, even if its code is buggy and throws an exception.
sub forked_tests {
    my $parent_cb = shift;
    my @child_cb = @_;

    for my $cb (@child_cb) {
        if (!fork) {
            eval {
                $cb->();
            };
            if ($@) {
                fail "child failed: $@";
                exit 1;
            }
            exit 0;
        }
    }

    eval {
        $parent_cb->();
    };
    if ($@) {
        fail "parent failed: $@";
    }
    wait_all;
    parent_ok();
}

sub single_nonblocking_lock :Tests {
    forked_tests(sub {
        tsleep 1;
        ok((not defined lockf("tfiles/lock", {blocking => 0})), 'undef when already locked');
    }, sub {
        my $lock = lockf("tfiles/lock");
        tsleep 2;
    });
}

sub shared_lock :Tests {
    forked_tests(sub {
        tsleep 1;
        ok(!lockf("tfiles/lock", {blocking => 0}), "don't acquire lock when shared lock exists");
    }, sub {
        my $lock = lockf("tfiles/lock", {shared => 1});
        tsleep 2;
    }, sub {
        tsleep 1;
        child_ok(lockf("tfiles/lock", {shared => 1, blocking => 0}), "acquire shared lock twice");
    });
}

sub some_more :Tests {
    forked_tests(sub {
        my $lock;
        ok(!exception { $lock = lockf("tfiles/lock", {blocking => 0}) }, "get nonblocking lock");
    }, sub {
        tsleep 3;
        child_ok(
            not(lockf("tfiles/lock", {blocking => 0})),
            "undef when already locked"
        );
    }, sub {
        tsleep 1;
        my $lock;
        child_ok($lock = lockf("tfiles/lock"), "blocking wait for lock");
        tsleep 2;
    });
}

sub share_unshare :Tests {
    forked_tests(sub {
        my $lock = lockf("tfiles/lock", {shared => 1, blocking => 0});
        tsleep 2; #+2s
        undef $lock;
        tsleep 1;
        ok(!lockf("tfiles/lock", {shared => 1, blocking => 0}), "don't get shared lock when exclusive lock exists");
        tsleep 3;
        ok(lockf("tfiles/lock", {shared => 1, blocking => 0}), "get shared lock when shared lock exists");
    }, sub {
        my $lock = lockf("tfiles/lock", {shared => 1, blocking => 0});
        tsleep 1; # +1s
        child_ok(!exception { $lock->unshare() }, "unshare shared lock"); # will wait 1 second, +2s
        tsleep 3; # +5s
        child_ok(!exception { $lock->share() }, "share exclusive lock"); # +5s
        tsleep 2; # +7s
    });
}

sub timeout :Tests {
    forked_tests(sub {
        sleep 1;
        ok(exception { lockf("tfiles/lock", {timeout => 0}) }, "timeout => 0 throws an exception");
        ok(exception { lockf("tfiles/lock", {timeout => 3}) }, "can't get lock in the first 3 seconds");
        ok(!exception { lockf("tfiles/lock", {timeout => 3}) }, "can get lock in the next 3 seconds");

        my $other_lock = lockf("tfiles/lock.other", {timeout => 0});
        ok($other_lock, "timeout => 0 works like nonblocking => 0");
    }, sub {
        my $lock = lockf("tfiles/lock");
        sleep 5; # timeout don't support float values, so we can't use tsleep here
    });
}

sub mode :Tests {
    my $state = lockf('tfiles/lock', { mode => 0765 });
    undef $state;

    my $mode = (stat('tfiles/lock'))[2];
    ok(($mode & 07777) == 0765, "mode set right");
}


sub multilock :Tests {
    forked_tests(sub {
        tsleep 1;
        ok(!exception { lockf_multi("tfiles/lock", 4) }, "can get multilock 4 of 4");
    }, sub {
        my $lockf1 = lockf_multi("tfiles/lock", 4);
        my $lockf2 = lockf_multi("tfiles/lock", 4);
        my $lockf3 = lockf_multi("tfiles/lock", 4);
        tsleep 3;
    });
}

sub more_multilock :Tests {
    forked_tests(sub {
        tsleep 1;
        is lockf_multi("tfiles/lock", 4), undef, "can't get multilock 5 of 4, but don't throw exception";
    }, sub {
        my $lockf1 = lockf_multi("tfiles/lock", 4);
        my $lockf2 = lockf_multi("tfiles/lock", 4);
        my $lockf3 = lockf_multi("tfiles/lock", 4);
        my $lockf4 = lockf_multi("tfiles/lock", 4);
        tsleep 3;
    });
}

sub and_more_multilock :Tests {
    for my $remove (0, 1) {
        forked_tests(sub {
            tsleep 1;
            my $msg = $remove ? "(remove => 1)" : "";
            ok(!lockf_multi("tfiles/lock", 2), "can't get multilock for 2 when 4 are locked $msg");
            ok(!lockf_multi("tfiles/lock", 4), "can't get multilock for 4 when 4 are locked $msg");
            ok(lockf_multi("tfiles/lock", 5), "can get multilock for 5 when 4 are locked $msg");
        }, sub {
            my @locks;
            foreach(0..6) {
                push @locks, lockf_multi("tfiles/lock", 7, { remove => $remove });
            }
            delete @locks[1..3];
            tsleep 3;
        });
    }
}

sub multilock_no_exceptions :Tests {
    ok(exception { my $lockf1 = lockf_multi("tfiles/dir/lock", 4, 1) }, 'lockf_multi throws exception even with no_exceptions flag if error is not about lock availability');
}


sub name :Tests {
    my $lock = lockf("tfiles/lock");
    ok($lock->name() eq "tfiles/lock", "name OK");
}

sub test_lockf_any :Tests {
    my @files = ("tfiles/lock.foo", "tfiles/lock.bar");

    my $lock1 = lockf_any(\@files);
    my $lock2 = lockf_any(\@files);

    ok(!lockf_any(\@files), "lockf_any won't lock what it should not");
    ok(($lock1->name() eq 'tfiles/lock.foo') && ($lock2->name() eq 'tfiles/lock.bar'), "names and order are fine");
}

sub alarm :Tests {
    # timeout option don't support float values because alarm() from Time::HiRes is buggy, so we can't use tsleep here
    forked_tests(sub {
        my $alarmed = 0;
        local $SIG{ALRM} = sub {
            $alarmed++;
        };
        sleep 1;
        alarm(6);
        ok(exception { lockf("tfiles/lock", { timeout => 2 }) }, "timeout 2 fails");
        ok(!exception { lockf("tfiles/lock", { timeout => 2 }) }, "timeout 4 succeeds");
        sleep 3;
        ok($alarmed == 1, "timeout preserves external alarms");

        if (!fork) {
            my $lock = lockf("tfiles/lock");
            tsleep 2;
            exit(0);
        }
        alarm(1);
        ok(!exception { lockf("tfiles/lock", { timeout => 3 }) }, "timeout 3 succeeds");
        sleep 2;
        ok($alarmed == 2, "alarms that fired during timeout are preserved thou delayed");
    }, sub {
        my $lock = lockf("tfiles/lock");
        sleep 4;
    });
}

sub remove :Tests {
    my $time = time;
    system("echo 0 > tfiles/1 && echo 0 > tfiles/2");
    my $lockf = lockf("tfiles/lock", { remove => 1 });
    undef $lockf;
    ok(!(-e "tfiles/lock"), "'remove' option");

    for (1..5) {
        fork and next;
        while () {
            last if time >= $time + 2;
            my $lockf = lockf("tfiles/lock", { remove => 1 });
            my @fh = map { open(my $fh, "+<", "tfiles/$_"); $fh } (1..2);
            my @x = map { scalar(<$_>) } @fh;
            $_++ for @x;
            seek ($_, 0, 0) or die "seek failed: $!" for @fh;
            # save in a reverse order
            xprint $fh[1], $x[1];
            xprint $fh[0], $x[0];
            close $_ for @fh;
        }
        exit 0;
    }

    wait_all;
    cmp_ok(xqx("cat tfiles/1"), "==", xqx("cat tfiles/2"), "unlink/lockf race");
}

sub special_symbols :Tests {
    my $l1 = lockf_multi("tfiles/x[y]", 1);
    ok(-e "tfiles/x[y].0", "filename");
    is(exception { lockf_multi("tfiles/x[y]", 1) }, undef, "glob quoting");
}

sub invalid_parameters :Tests {
    like
        exception { lockf('tfiles/lock', { foo => 1 }) },
        qr/Unexpected options: foo/;
}

__PACKAGE__->new->runtests;

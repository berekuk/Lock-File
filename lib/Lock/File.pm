package Lock::File;

# ABSTRACT: file locker with an automatic out-of-scope unlocking mechanism

=head1 SYNOPSIS

    use Yandex::Lockf 3.0;
    $lock = lockf($filehandle);
    $lock->unlockf();
    $lock = lockf($filename);
    undef $lock; # unlocks either

    $lock = lockf_multi($fname, $count); # will try to lock on files "$fname.0", "$fname.1" .. "$fname.".($count-1)
    $lock = lockf_multi($fname, $count, $no_exception); # don't throw exception if fails

    $lock = lockf_any('foo', 'bar');

    use Yandex::Lockf 2.0;
    $lock = lockf($filehandle);


=head1 DESCRIPTION

C<lockf> is a perlfunc C<flock> wrapper. The lock is autotamically released as soon as the assotiated object is
no longer referenced.

C<lockf_multi> makes non-blocking C<lockf> calls for multiple files and throws and exception if all are locked.

=head1 METHODS

=over

=item B<lockf($file, $options)>

Create an Lockf instance. Always save the result in some variable(s), otherwise the lock will be released immediately.

The lock is automatically released when all the references to the Lockf object are lost. The lockf mandatory parameter
can be either a string representing a filename or a reference to an already opened filehandle. The second optional
parameter is a hash of boolean options. Supported options are:

=over

=item I<shared>

OFF by default. Tells to achieve a shared lock. If not set, an exclusive lock is requested.

=item I<blocking>

ON by default. If unset, a non-blocking mode of flock is used. If this flock fails because the lock is already held by some other process,
 the behavior is determined by the value of 'version' parameter. If the failure reason is somewhat different, permissions problems or the
 absence of a target file directory for example, an exception is raisen.

=item I<version>

The default value is 2. This may be overridden by the import directive like C<use Yandex::Lockf 3.0;>. And specifying an explicit
 I<version> parameter in a I<lockf> call overrides yet more. In version 2 any non-blocking flock failures raise an exception. In versions >= 3 in
 a non-blocking mode undef is returned if the file is already locked. Other errors still raise exceptions.

=item I<timeout>

Undef by default. If set, specifies the wait timeout for acquiring the blocking lock. The value of 0 is allowed, which emulates the behavior
 of blocking => 0, version => 2 combination, reguardless of the actual version value.

=item I<mode>

Undef by default. If set, a chmod with the specified mode is performed on a newly created file. Ignored when filehandle is passed instead of a filename.

=item I<remove>

OFF by default. If set, the lock file will be deleted before unlocking.

=back

=item B<lockf_multi($file, $max, $options)>

Calls non-blocking C<lockf>'s for files from $fname.0 to $fname.$count, and returns Yandex::Lockf object for first
successful lock.

Only one option I<version> is currently supported, it works like version from C<lockf> call.

=item B<lockf_any($filenames, $options)>

Same as lockf_multi, but accepts arrayref of filenames.

=back

=head1 METHODS

=over

=item B<unlockf()>

Force the lock to be released independent of how many references to the object are still alive.

=item B<share()>

Transform exclusive lock to shared.

=item B<unshare()>

Transform shared lock to exclusive. Can block if other shared/exclusive locks are held by some other processes.

=item B<name()>

Gives the name of the file, as it was when the lock was taken.

=back

=head1 FAQ

=over

=item I<Yet another locking module? Why?>

There're L<tons of file locking modules|https://metacpan.org/search?q=lock> on CPAN. Last time I counted, there were at least 17.

And yet, every time I tried to find a replacement for our in-house code on which this module is based, every one of them had quirks which I found too uncomfortable. I had to copy our code as L<Ubic::Lockf> when I opensourced Ubic.

I wanted to do the review all those modules, L<neilb style|http://neilb.org/reviews/>. I never got around to that, and then I realized how much this task holds me back in releasing other useful stuff.

So... sorry for bringing yet-another-file-locking module into the world.

=item I<Why Lock::File instead of File::Lock?>

First, there's L<File::Lock::Multi>, which is completely unrelated to this module.

Second, there are so many locking modules that choosing a good name is *hard*.

Third, maybe I'm going to release L<Lock::Zookeeper> with the similar interface in the future.

=back

=cut

use strict;
use Fcntl qw(:flock);

use Yandex::Version '{{DEBIAN_VERSION}}';

use Yandex::Lockf::Alarm;

use Yandex::Logger;
use Yandex::X 1.2.0; # atomic xopen+chmod
use Params::Validate;
use POSIX qw(:errno_h);
use Carp;

use version;

sub DESTROY ($) {
    local $@;
    my ($self) = @_;
    my $fh = $self->{_fh};
    return unless defined $fh; # already released
    unlink $self->{_fname} if $self->{_remove} and $self->{_fname};
    flock $fh, LOCK_UN; #FIXME: we use flock instead of xlockf, because sometimes this handle appeares to be already closed (see Yandex::AcceptUtils::Log)
    delete $self->{_fh}; # closes the file if opened by us
}

my %defaults = (
    shared => 0,
    blocking => 1,
    version => 2,
    timeout => undef,
    mode => undef,
    remove => 0,
);

sub lockf ($;$) {
    my ($param, $opts) = validate_pos(@_, 1, 0);
    $opts ||= {};
    $opts = validate(@{ [ $opts ] }, {
        blocking => 0,
        nonblocking => 0,
        shared => 0,
        silent => 0, # deprecated option, does nothing
        timeout => 0,
        mode => 0,
        version => 0,
        remove => 0,
    });
    $opts = {%defaults, %$opts};
    $opts->{blocking} = not $opts->{nonblocking} if defined $opts->{nonblocking};

    my ($fh, $fname);
    if (ref $param eq "") { # filename instead of filehandle
        $fname = $param;
    } else {
        $fh = $param;
    }

    $fh = _lockf_and_check($fh, $fname, $opts);
    unless ($fh) {
        croak "flock $fname failed: $!" unless $opts->{version} >= 3;
        return; # version 3 returns undef when non-blocking lockf fails
    }

    return bless {
        _fh => $fh,
        _fname => $fname,
        _remove => $opts->{remove},
    } => __PACKAGE__;
}

sub _lockf_and_check {
    my ($fh, $fname, $opts) = @_;

    unless ($fname) { # no unlink/lockf race when locking an already opened filehandle
        return _lockf(@_) ? $fh : undef;
    }

    while () {
        my $mode = ">>";
        $mode .= $opts->{mode} if $opts->{mode};
        $fh = xopen $mode, $fname; # reopen
        my $lockf = _lockf($fh, $fname, $opts);
        return unless $lockf;

        unless (-e $fname) {
            DEBUG "$fname: locked but removed";
            next;
        }
        unless ((stat $fh)[1] eq (stat $fname)[1]) {
            DEBUG "$fname: locked but removed and created back";
            next;
        }
        return $fh;
    }
}

sub _lockf {
    my ($fh, $fname, $opts) = @_;

    $fname ||= ''; # TODO - discover $fname from $fh, it's possible in most cases with some /proc magic

    my $mode = ($opts->{shared} ? LOCK_SH : LOCK_EX);

    if (
        not $opts->{blocking}
        or (defined $opts->{timeout} and not $opts->{timeout}) # timeout=0
    ) {
        return 1 if flock ($fh, $mode | LOCK_NB);
        return 0 if ($! == EWOULDBLOCK);
        croak "flock ".($fname || '')." failed: $!";
    }

    unless (flock ($fh, $mode | LOCK_NB)) {
        my $msg = "$fname already locked, wait...";
        DEBUG $msg;
    } else {
        return 1;
    }

    if ($opts->{timeout}) {
        local $SIG{ALRM} = sub { croak "flock $fname failed: timed out" };
        my $alarm = Yandex::Lockf::Alarm->new($opts->{timeout});
        xflock $fh, $mode;
    } else {
        xflock $fh, $mode;
    }
    return 1;
}

sub name($)
{
    my $self = shift();
    return $self->{_fname};
}

sub share($)
{
    my ($self) = @_;
    xflock $self->{_fh}, LOCK_SH;
}

sub unshare($)
{
    my ($self) = @_;
    xflock $self->{_fh}, LOCK_EX;
}

sub unlockf ($) {
    my ($self) = @_;
    $self->DESTROY();
}

my %multi_defaults = (
    version => 2,
    remove => 0,
);

sub lockf_multi ($$;$) {
    my ($fname, $max, $opts) = @_;
    if ($opts and not ref $opts) {
        $opts = { version => 3 };
    }
    if ($opts) {
        $opts = validate(@{ [$opts] }, {
            version => 0,
            nonblocking => 0,
            silent => 0,
            remove => 0,
        });
        for (qw/ nonblocking silent /) {
            WARN "lockf_multi doesn't support '$_' option" if defined $opts->{$_};
        }
    }
    else {
        $opts = {};
    }
    $opts = {%multi_defaults, %$opts};

    my $metalock = lockf("$fname.meta", { remove => 1 }); # to make sure no one will mess up the things

    my %flist = map { $_=>1 } grep { /^\Q$fname\E\.\d+$/ } glob "\Q$fname\E.*";

    my $locked = 0;
    my $ret;
    for my $file (keys %flist) # try to get lock on existing file
    {
        my $lockf = lockf($file, { blocking => 0, version => 3, remove => $opts->{remove} });
        $locked++ unless $lockf;
        $ret ||= $lockf;
        if ($locked >= $max) {
            undef $ret;
            last;
        }
    }

    if ($locked < $max and not $ret) {
        for my $i (0 .. ($max-1)) {
            my $file = "$fname.$i";
            next if $flist{$file};
            my $lockf = lockf($file, { blocking => 0, version => 3, remove => $opts->{remove} });
            die unless $lockf; # mystery
            $ret = $lockf;
            last;
        }
    }

    return $ret if defined $ret;
    return undef if $opts->{version} >= 3;
    croak "lockf_multi($fname, $max) failed - all files locked";
}


sub lockf_any($;$) {
    my ($flist, $opts) = @_;
    if ($opts and not ref $opts) {
        $opts = { version => 3 };
    }
    if ($opts) {
        $opts = validate(@{ [$opts] }, {
            version => 0,
            remove => 0,
        });
    }
    else {
        $opts = {};
    }
    $opts = {%multi_defaults, %$opts};

    foreach my $fname (@$flist)
    {
        my $lockf = lockf($fname, { blocking => 0, version => 3, remove => $opts->{remove} });
        return $lockf if $lockf;
    }

    return undef if $opts->{version} >= 3;
    croak "lockf_any couldn't get lock";
}


my @all_exports = qw( lockf unlockf lockf_multi lockf_any );
my $requested_version;
sub import {
    my $package = shift;
    my ($caller) = caller;
    my @exports = @_;
    unless (@exports) {
        @exports = @all_exports;
    }

    my $version = $requested_version;
    undef $requested_version;

    for my $export (@exports) {
        no strict 'refs';
        if ($export eq 'lockf' and $version and $version >= qv('3.0')) {
            *{$caller."::".$export} = sub {
                my ($file, $opts) = @_;
                $opts->{version} ||= 3;
                lockf($file, $opts);
            };
        }
        elsif ($export eq 'lockf_multi' and $version and $version >= qv('3.0')) {
            *{$caller."::".$export} = sub {
                my ($fname, $max, $opts) = @_;
                if (ref $opts) {
                    # hash
                    $opts->{version} ||= 3;
                }
                else {
                    # simple scalar value, $no_exceptions
                    WARN 'lockf_multi 3rd parameter should be hashref, $no_exceptions is deprecated' if $opts;
                    $opts = { version => 3 };
                }
                lockf_multi($fname, $max, $opts);
            };
        }
        elsif ($export eq 'lockf_any' and $version and $version >= qv('3.0')) {
            *{$caller."::".$export} = sub {
                my ($flist, $opts) = @_;
                if (ref $opts) {
                    # hash
                    $opts->{version} ||= 3;
                }
                else {
                    # simple scalar value, $no_exceptions
                    WARN 'lockf_any 3rd parameter should be hashref, $no_exceptions is deprecated' if $opts;
                    $opts = { version => 3 };
                }
                lockf_any($flist, $opts);
            };
        }
        elsif (grep { $_ eq $export } @all_exports) {
            *{$caller."::".$export} = \&{$export};
        }
        else {
            die "$export is not exported by Yandex::Lockf module";
        }
    }
}

sub VERSION {
    $requested_version = $_[1];
    shift->SUPER::VERSION(@_);
}

1;

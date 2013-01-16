package Lock::File;

# ABSTRACT: file locker with an automatic out-of-scope unlocking mechanism

=head1 SYNOPSIS

    use Lock::File qw(lockf);

    # blocking mode is default
    $lock = lockf('/var/lock/my_script.lock');

    # unlock
    undef $lock;
    # or:
    $lock->unlockf();

    $lock = lockf('./my.lock', { blocking => 0 }) or die "Already locked";

    # lock an open file:
    $lock = lockf($fh);

    $lock = lockf_multi('./my.lock', 5); # will try to lock on files "my.lock.0", "my.lock.1" .. "my.lock.4"

    $lock = lockf_any('foo', 'bar');


=head1 DESCRIPTION

C<lockf> is a perlfunc C<flock> wrapper. The lock is autotamically released as soon as the assotiated object is
no longer referenced.

C<lockf_multi> makes non-blocking C<lockf> calls for multiple files and throws and exception if all are locked.

=head1 FUNCTIONS

=over

=item B<lockf($file, $options)>

Create a Lock instance. Always save the result in some variable(s), otherwise the lock will be released immediately.

The lock is automatically released when all the references to the Lockf object are lost. The lockf mandatory parameter
can be either a string representing a filename or a reference to an already opened filehandle. The second optional
parameter is a hash of boolean options. Supported options are:

=over

=item I<shared>

OFF by default.
Tells to achieve a shared lock. If not set, an exclusive lock is requested.

=item I<blocking>

ON by default.
If unset, a non-blocking mode of flock is used. If this flock fails because the lock is already held by some other process,
C<undef> is returned. If the failure reason is somewhat different, e.g. permissions problems or the absence of a target file directory, an exception is thrown.

=item I<timeout>

Unset by default.
If set, specifies the wait timeout for acquiring the blocking lock.

Throws an exception on timeout.

The value of 0 is equivalent to C<< blocking => 0 >> option, except that it throws an exception instead of returning undef if the file is already locked.

=item I<mode>

Undef by default.
If set, a chmod with the specified mode is performed on a newly created file. Ignored when filehandle is passed instead of a filename.

=item I<remove>

OFF by default.
If set, the lock file will be deleted before unlocking.

=back

=item B<lockf_multi($file, $max, $options)>

Calls non-blocking C<lockf>'s for files from C<$fname.0> to C<$fname.$max-1>, and returns a C<Lock::File> object for the first successful lock.

Only I<remove> and I<mode> options are supported.

=item B<lockf_any($filenames, $options)>

Same as C<lockf_multi>, but accepts arrayref of filenames.

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

I wanted to do the review of all those modules, L<neilb style|http://neilb.org/reviews/>. I never got around to that, and then I realized how much this task holds me back from releasing other useful stuff.

So... sorry for bringing yet-another-file-locking module into the world.

=item I<Why Lock::File instead of File::Lock?>

First, there's L<File::Lock::Multi>, which is completely unrelated to this module.

Second, there are so many locking modules that choosing a good name is *hard*.

Third, maybe I'm going to release L<Lock::Zookeeper> with the similar interface in the future.

=back

=cut

use strict;
no warnings;
use Fcntl qw(:DEFAULT :flock);

use Lock::File::Alarm;

use Log::Any qw($log);
use Params::Validate;
use POSIX qw(:errno_h);
use Carp;

use autodie qw(open);

use base qw(Exporter);
our @EXPORT_OK = qw( lockf unlockf lockf_multi lockf_any );
our %EXPORT_TAGS = (all => \@EXPORT_OK);

sub DESTROY {
    local $@;
    my ($self) = @_;
    my $fh = $self->{_fh};
    return unless defined $fh; # already released
    unlink $self->{_fname} if $self->{_remove} and $self->{_fname};
    flock $fh, LOCK_UN; # don't check result code - sometimes this handle appeares to be already closed
    delete $self->{_fh}; # closes the file if opened by us
}

my %defaults = (
    shared => 0,
    blocking => 1,
    timeout => undef,
    mode => undef,
    remove => 0,
);

sub lockf ($;$) {
    my ($param, $opts) = validate_pos(@_, 1, 0);
    $opts ||= {};
    $opts = validate(@{ [ $opts ] }, {
        blocking => 0,
        shared => 0,
        timeout => 0,
        mode => 0,
        remove => 0,
    });
    $opts = {%defaults, %$opts};

    my ($fh, $fname);
    if (ref $param eq "") { # filename instead of filehandle
        $fname = $param;
    } else {
        $fh = $param;
    }

    $fh = _lockf_and_check($fh, $fname, $opts);
    unless ($fh) {
        return;
    }

    return bless {
        _fh => $fh,
        _fname => $fname,
        _remove => $opts->{remove},
    } => __PACKAGE__;
}

sub _open {
    my ($chmod, $fname) = @_;

    my $fh;
    my $res;
    if (defined $chmod) {
        $chmod = oct($chmod) if $chmod =~ s/^0//;
        my $mode = O_WRONLY|O_CREAT|O_APPEND;
        my $umask = umask(0);
        $res = sysopen $fh, $fname, $mode, $chmod;
        umask($umask);
    }
    else {
        $res = open $fh, '>>', $fname;
    }
    die "open $fname with mode $chmod failed: ", _log_message($!) unless $res;
    return $fh;
}

sub _lockf_and_check {
    my ($fh, $fname, $opts) = @_;

    unless (defined $fname) { # no unlink/lockf race when locking an already opened filehandle
        return _lockf(@_) ? $fh : undef;
    }

    while () {
        $fh = _open($opts->{mode}, $fname);
        my $lockf = _lockf($fh, $fname, $opts);
        return unless $lockf;

        unless (-e $fname) {
            $log->debug("$fname: locked but removed");
            next;
        }
        unless ((stat $fh)[1] eq (stat $fname)[1]) {
            $log->debug("$fname: locked but removed and created back");
            next;
        }
        return $fh;
    }
}

sub _xflock {
    my ($fh, $mode) = @_;
    flock $fh, $mode or die "flock failed: $!";
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
        $log->debug($msg);
    } else {
        return 1;
    }

    if ($opts->{timeout}) {
        local $SIG{ALRM} = sub { croak "flock $fname failed: timed out" };
        my $alarm = Lock::File::Alarm->new($opts->{timeout});
        _xflock($fh, $mode);
    } else {
        _xflock($fh, $mode);
    }
    return 1;
}

sub name {
    my $self = shift;
    return $self->{_fname};
}

sub share {
    my $self = shift;
    _xflock($self->{_fh}, LOCK_SH);
}

sub unshare {
    my $self = shift;
    _xflock($self->{_fh}, LOCK_EX);
}

sub unlockf {
    my $self = shift;
    $self->DESTROY();
}

sub lockf_multi ($$;$) {
    my ($fname, $max, $opts) = @_;
    if ($opts) {
        $opts = validate(@{ [$opts] }, {
            remove => 0,
            mode => 0,
        });
    }
    else {
        $opts = {};
    }

    # to make sure no one will mess up the things
    # TODO - apply opts to metalock too?
    my $metalock = lockf("$fname.meta", { remove => 1 });

    my %flist = map { $_ => 1 } grep { /^\Q$fname\E\.\d+$/ } glob "\Q$fname\E.*";

    my $locked = 0;
    my $ret;
    for my $file (keys %flist) # try to get lock on existing file
    {
        my $lockf = lockf($file, { blocking => 0, %$opts });
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
            my $lockf = lockf($file, { blocking => 0, %$opts });
            die unless $lockf; # mystery - FIXME
            $ret = $lockf;
            last;
        }
    }

    return $ret if defined $ret;
    return undef;
}


sub lockf_any ($;$) {
    my ($flist, $opts) = @_;
    if ($opts) {
        $opts = validate(@{ [$opts] }, {
            remove => 0,
            mode => 0,
        });
    }
    else {
        $opts = {};
    }

    for my $fname (@$flist)
    {
        my $lockf = lockf($fname, { blocking => 0, remove => $opts->{remove} });
        return $lockf if $lockf;
    }

    return undef;
}

1;

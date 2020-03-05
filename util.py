# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Utility functions for the clean installer
#
# written by Andrew Peace

import os
import os.path
import subprocess
import urllib
import urllib2
import urlparse
import shutil
import re
import datetime
import time
import random
import string
import tempfile
import errno
from version import *
from xcp import logger

random.seed()

_dev_null_fh = None

###
# string helpers

def elide(text, max_len):
    return text[:max(max_len, 3) - 3] + "..." if len(text) > max_len else text

###
# directory/tree management

def assertDir(dirname):
    # make sure there isn't already a file there:
    assert not (os.path.exists(dirname) and not os.path.isdir(dirname))

    # does the specified directory exist?
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

def assertDirs(*dirnames):
    for d in dirnames:
        assertDir(d)

def copyFile(source, dest):
    assert os.path.isfile(source)
    assert os.path.isdir(dest)

    assert runCmd2(['cp', '-f', source, '%s/' % dest]) == 0

def copyFilesFromDir(sourcedir, dest):
    assert os.path.isdir(sourcedir)
    assert os.path.isdir(dest)

    files = os.listdir(sourcedir)
    for f in files:
        assert runCmd2(['cp', '-a', '%s/%s' % (sourcedir, f), '%s/' % dest]) == 0

###
# shell

def runCmd2(command, with_stdout=False, with_stderr=False, inputtext=None):

    cmd = subprocess.Popen(command, bufsize=1,
                           stdin=(inputtext and subprocess.PIPE or None),
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE,
                           shell=isinstance(command, str),
                           close_fds=True)

#     if inputtext:
#      (out, err) = cmd.communicate(inputtext)
#         rv = cmd.returncode
#     else:
#         (stdout, stderr) = (cmd.stdout, cmd.stderr)
#         for line in stdout:
#             out += line
#         for line in stderr:
#             err += line
#         rv = cmd.wait()

    # the above has a deadlock condition.
    # The following should suffice in all cases
    (out, err) = cmd.communicate(inputtext)
    rv = cmd.returncode

    l = "ran %s; rc %d" % (str(command), rv)
    if inputtext:
        l += " with input %s" % inputtext
    if out != "":
        l += "\nSTANDARD OUT:\n" + out
    if err != "":
        l += "\nSTANDARD ERROR:\n" + err
    logger.log(l)

    if with_stdout and with_stderr:
        return rv, out, err
    elif with_stdout:
        return rv, out
    elif with_stderr:
        return rv, err
    return rv

###
# make file system

def mkfs(fstype, partition, options=None, wipe=True):
    if wipe:
        rc, err = runCmd2(['wipefs', '-a', partition], with_stderr=True)
        if rc != 0:
            raise Exception("err: '%s'" % err)

    mkfs_cmd = ['mkfs.%s' % fstype , partition]
    if options:
        mkfs_cmd.extend(options)
    rc, err = runCmd2(mkfs_cmd, with_stderr=True)
    if rc != 0:
        raise Exception("err: '%s'" % err)

###
# mounting/unmounting

class MountFailureException(Exception):
    pass

def pidof(name):
    def is_pid(s):
        for c in s:
            if not c in string.digits: return False
        return True
    def has_name(pid):
        try:
            return os.path.basename(open('/proc/%s/cmdline' % pid).read().split('\0')[0]) == name
        except:
            return False
    pids = filter(is_pid, os.listdir('/proc'))
    pids = filter(has_name, pids)
    return pids

def mount(dev, mountpoint, options=None, fstype=None):
    logger.log("Mounting %s to %s, options = %s, fstype = %s" % (dev, mountpoint, options, fstype))

    cmd = ['/bin/mount']
    if options:
        assert type(options) == list

    if fstype:
        cmd += ['-t', fstype]

    if options:
        cmd += ['-o', ",".join(options)]

    cmd.append(dev)
    cmd.append(mountpoint)

    rc, out, err = runCmd2(cmd, with_stdout=True, with_stderr=True)
    if rc != 0:
        raise MountFailureException("out: '%s' err: '%s'" % (out, err))

def bindMount(source, mountpoint):
    logger.log("Bind mounting %s to %s" % (source, mountpoint))

    cmd = [ '/bin/mount', '--bind', source, mountpoint]
    rc, out, err = runCmd2(cmd, with_stdout=True, with_stderr=True)
    if rc != 0:
        raise MountFailureException("out: '%s' err: '%s'" % (out, err))

def umount(mountpoint, force=False):
    logger.log("Unmounting %s (force = %s)" % (mountpoint, force))

    cmd = ['/bin/umount', '-d'] # -d option also removes the loop device (if present)
    if force:
        cmd.append('-f')
    cmd.append(mountpoint)

    rc = runCmd2(cmd)
    return rc

class TempMount:
    def __init__(self, device, tmp_prefix, options=None, fstype=None, boot_device=None, boot_mount_point=None):
        self.mounted = False
        self.mount_point = tempfile.mkdtemp(dir="/tmp", prefix=tmp_prefix)
        self.boot_mount_point = None
        self.boot_mounted = False
        try:
            mount(device, self.mount_point, options, fstype)

            if boot_device:
                # Determine where the boot device needs to be mounted by looking through fstab
                match = None
                bootfstype = None
                try:
                    with open(os.path.join(self.mount_point, 'etc', 'fstab'), 'r') as fstab:
                        for line in fstab:
                            m = re.search("\\s(/boot(/[^\\s]+)?)\\s+(\\w+)", line)
                            if m:
                                match = m.group(1)
                                bootfstype = m.group(3)
                except IOError as e:
                    if e.errno != errno.ENOENT:
                        raise

                if match:
                    self.boot_mount_point = self.mount_point + match
                elif boot_mount_point:
                    self.boot_mount_point = self.mount_point + boot_mount_point

                if self.boot_mount_point:
                    assertDir(self.boot_mount_point)
                    mount(boot_device, self.boot_mount_point, options, bootfstype)
                    self.boot_mounted = True
        except:
            os.rmdir(self.mount_point)
            raise
        self.mounted = True

    def unmount(self):
        if self.boot_mounted:
            umount(self.boot_mount_point)
            self.boot_mounted = False
        if self.mounted:
            umount(self.mount_point)
            self.mounted = False
        if os.path.isdir(self.mount_point):
            os.rmdir(self.mount_point)

###
# fetching of remote files

class InvalidSource(Exception):
    pass

# source may be
#  http://blah
#  ftp://blah
#  file://blah
#  nfs://server:/path/blah
def fetchFile(source, dest):
    cleanup_dirs = []

    try:
        # if it's NFS, then mount the NFS server then treat like
        # file://:
        if source[:4] == 'nfs:':
            # work out the components:
            [_, server, path] = source.split(':')
            if server[:2] != '//':
                raise InvalidSource("Did not start {ftp,http,file,nfs}://")
            server = server[2:]
            dirpart = os.path.dirname(path)
            if dirpart[0] != '/':
                raise InvalidSource("Directory part of NFS path was not an absolute path.")
            filepart = os.path.basename(path)
            logger.log("Split nfs path into server: %s, directory: %s, file: %s." % (server, dirpart, filepart))

            # make a mountpoint:
            mntpoint = tempfile.mkdtemp(dir='/tmp', prefix='fetchfile-nfs-')
            mount('%s:%s' % (server, dirpart), mntpoint, fstype="nfs", options=['ro'])
            cleanup_dirs.append(mntpoint)
            source = 'file://%s/%s' % (mntpoint, filepart)

        if source[:5] == 'http:' or \
               source[:6] == 'https:' or \
               source[:5] == 'file:' or \
               source[:4] == 'ftp:':
            # This something that can be fetched using urllib2:
            fd = urllib2.urlopen(source)
            fd_dest = open(dest, 'w')
            shutil.copyfileobj(fd, fd_dest)
            fd_dest.close()
            fd.close()
        else:
            raise InvalidSource("Unknown source type.")

    finally:
        for d in cleanup_dirs:
            umount(d)
            os.rmdir(d)

def getUUID():
    rc, out = runCmd2(['uuidgen'], with_stdout=True)
    assert rc == 0

    return out.strip()

def mkRandomHostname():
    """ Generate a random hostname of the form xenserver-AAAAAAAA """
    s = "".join([random.choice(string.ascii_lowercase) for x in range(8)])
    return "%s-%s" % (BRAND_SERVER.split()[0].lower(),s)

def splitArgs(argsIn, array_args=()):
    """ Split argument array into dictionary

    [ '--alpha', '--beta=42' ]

    becomes

    { '--alpha': None, '--beta': '42' }"""
    argsOut = {}
    for arg in argsIn:
        eq = arg.find('=')
        if eq == -1:
            argsOut[arg] = None
        else:
            k = arg[:eq]
            v = arg[eq+1:]
            if k in array_args:
                if k in argsOut:
                    argsOut[k].append(v)
                else:
                    argsOut[k] = [v]
            else:
                argsOut[k] = v

    return argsOut

def readKeyValueFile(filename, allowed_keys=None, strip_quotes=True):
    """ Reads a KEY=Value style file (e.g. xensource-inventory). Returns a
    dictionary of key/values in the file.  Not designed for use with large files
    as the file is read entirely into memory."""

    f = open(filename, "r")
    lines = [x.strip("\n") for x in f.readlines()]
    f.close()

    # remove lines that do not contain allowed keys
    if allowed_keys:
        lines = filter(lambda x: True in [x.startswith(y) for y in allowed_keys],
                       lines)

    defs = [ (l[:l.find("=")], l[(l.find("=") + 1):]) for l in lines ]

    if strip_quotes:
        def quotestrip(x):
            return x.strip("'")
        defs = [ (a, quotestrip(b)) for (a, b) in defs ]

    return dict(defs)

def dev_null():
    global _dev_null_fh
    if not _dev_null_fh:
        _dev_null_fh = open("/dev/null", 'r+')
    return _dev_null_fh

def udevadmCmd(cmd):
    if os.path.isfile('/sbin/udevadm'):
        return ['/sbin/udevadm', cmd]
    return ['udev' + cmd]

def udevsettleCmd():
    return udevadmCmd('settle')

def udevtriggerCmd():
    return udevadmCmd('trigger')

def udevinfoCmd():
    return udevadmCmd('info')

def randomLabelStr():
    return "".join([random.choice(string.ascii_lowercase) for x in range(6)])

def isNetInstall():
    with open('/proc/cmdline') as f:
        return 'netinstall' in f.read().split(' ')

def getLocalTime(timezone=None):
    if timezone:
        os.environ['TZ'] = timezone
        time.tzset()

    return datetime.datetime.now()

def setLocalTime(timestring, timezone=None):
    if timezone:
        os.environ['TZ'] = timezone
        time.tzset()

    assert runCmd2("date --set='%s'" % timestring) == 0

class URL(object):
    """A wrapper around a URL string.

    This is a wrapper around a URL string to avoid inadvertently logging
    the username/password of a URL string."""

    def __init__(self, url):
        self.url = url
        parts = urlparse.urlsplit(url)
        self.scheme = parts.scheme
        self.hostname = parts.hostname
        self.username = parts.username
        self.password = parts.password

    def __str__(self):
        """Returns the URL with username/password replaced with asterisks."""

        if self.username is not None and self.password is not None:
            return self.url.replace('%s:%s@' % (self.username, self.password), '***:***@', 1)
        elif self.username is not None:
            return self.url.replace('%s@' % self.username, '***:***@', 1)
        else:
            # Cannot have a password without a username
            assert self.password is None

            return self.url

    def getScheme(self):
        return self.scheme

    def getHostname(self):
        return self.hostname

    def getUsername(self):
        if self.username is None:
            return None
        return urllib.unquote(self.username)

    def getPassword(self):
        if self.password is None:
            return None
        return urllib.unquote(self.password)

    def getURL(self):
        """Get the full URL with username/password.

        The usage of this should be carefully audited to ensure it is not
        inadvertently logged (even as part of an exception)."""

        return self.url

    def getPlainURL(self):
        """Get the URL without username/password."""

        if self.username is not None and self.password is not None:
            return self.url.replace('%s:%s@' % (self.username, self.password), '', 1)
        elif self.username is not None:
            return self.url.replace('%s@' % self.username, '', 1)
        else:
            # Cannot have a password without a username
            assert self.password is None

            return self.url

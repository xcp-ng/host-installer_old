# Copyright (c) Citrix Systems 2009.  All rights reserved.
# Xen, the Xen logo, XenCenter, XenMotion are trademarks or registered
# trademarks of Citrix Systems, Inc., in the United States and other
# countries.

import constants
import os
import stat
import tempfile
import util
import xelogging

script_dict = {}

def add_script(stage, url):
    if stage not in script_dict:
        script_dict[stage] = []
    script_dict[stage].append(url)

def run_scripts(stage, *args):
    if stage not in script_dict:
        return

    for script in script_dict[stage]:
        run_script(script, stage, *args)

def run_script(script, stage, *args):
    xelogging.log("Running script for stage %s: %s %s" % (stage, script, ' '.join(args)))

    util.assertDir(constants.SCRIPTS_DIR)
    fd, local_name = tempfile.mkstemp(prefix = stage, dir = constants.SCRIPTS_DIR)
    try:
        util.fetchFile(script, local_name)

        # check the interpreter
        fh = os.fdopen(fd)
        fh.seek(0)
        line = fh.readline(40)
        fh.close()
    except:
        raise RuntimeError, "Unable to fetch script %s" % script

    if not line.startswith('#!'):
        raise RuntimeError, "Missing interpreter in %s." % script
    interp = line[2:].split()
    if interp[0] == '/usr/bin/env':
        if len (interp) < 2 or interp[1] not in ['python']:
            raise RuntimeError, "Invalid interpreter %s in %s." % (interp[1], script)
    elif interp[0] not in ['/bin/sh', '/bin/bash', '/usr/bin/python']:
        raise RuntimeError, "Invalid interpreter %s in %s." % (interp[0], script)
        
    cmd = [local_name]
    cmd.extend(args)
    os.chmod(local_name, stat.S_IRUSR | stat.S_IXUSR)
    os.environ['XS_STAGE'] = stage
    rc, out, err = util.runCmd2(cmd, with_stdout = True, with_stderr = True)
    xelogging.log("Script returned %d" % rc)
    # keep script, will be collected in support tarball

    return rc, out, err

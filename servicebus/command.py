import subprocess
import logging


def cmd(cmd):
    """
    Execute a command, return stdout and stderr in tuple
    """
    logging.debug("Execute Command: %s" % cmd)
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return (process.stdout.read(), process.stderr.read())


def get_host_name():
    out, err = cmd('hostname')
    return out.strip()

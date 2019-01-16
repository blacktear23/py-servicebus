import socket
import logging
import subprocess


def cmd(cmd_str):
    """
    Execute a command, return stdout and stderr in tuple
    """
    logging.debug("Execute Command: %s" % cmd_str)
    process = None
    try:
        process = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        out, err = process.communicate()
        return out.decode('utf-8'), err.decode('utf-8')
    finally:
        if process is not None:
            process.stdout.close()
            process.stderr.close()


def cmd2(cmd_str):
    """
    Execute a command return retcode, stdout and stderr in tuple
    """
    logging.debug("Execute Command: %s" % cmd_str)
    process = None
    try:
        process = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        out, err = process.communicate()
        ret_code = process.returncode
        return ret_code, out.decode('utf-8'), err.decode('utf-8')
    finally:
        if process is not None:
            process.stdout.close()
            process.stderr.close()


def get_host_name():
    return socket.gethostname()

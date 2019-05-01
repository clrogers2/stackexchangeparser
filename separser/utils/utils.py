import logging
import winreg
import shutil
import os
import sys
import threading
import subprocess

logger = logging.getLogger("StackExchangeParser")
syslog = logging.FileHandler(filename='./separse.log', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s %(name)s - %(levelname)s:%(message)s')
syslog.setFormatter(formatter)
logger.setLevel("INFO")
logger.addHandler(syslog)


def log(message):
    logger.info(message)


def find_program_win(program_to_find='SOFTWARE\\7-Zip'):

    try:
        h_key = winreg.CreateKey(winreg.HKEY_LOCAL_MACHINE, program_to_find)
        try:
            prog_path = (winreg.QueryValueEx(h_key, 'Path'))[0]
            return prog_path
        except OSError:
            log("7-Zip isn't correctly installed!! ")
            return None
    except PermissionError:
        log("7-Zip not found!! ")
        return None


def find_program_other(cmd='7z'):
    return shutil.which(cmd=cmd)


def capture_7zip_stdout(call):

    def create_dict(output):
        output_dict = {}
        files_list = output.split(os.linesep+os.linesep)
        for file in files_list:
            temp_dict = {}
            elements = file.split(os.linesep)
            if len(elements) > 1:
                for element in elements:
                    items = element.split('=')
                    key = items[0].strip()
                    value = items[1].strip()
                    temp_dict[key] = value
                output_dict[temp_dict['Path']] = temp_dict
        return output_dict

    stdout_fileno = sys.stdout.fileno()
    stdout_save = os.dup(stdout_fileno)
    stdout_pipe = os.pipe()
    os.dup2(stdout_pipe[1], stdout_fileno)
    os.close(stdout_pipe[1])

    captured_stdout = ''

    def drain_pipe():
        nonlocal captured_stdout
        while True:
            data = os.read(stdout_pipe[0], 1024)
            if not data:
                break
            captured_stdout += data.decode('utf-8')

    t = threading.Thread(target=drain_pipe)
    t.start()

    subprocess.call(call)

    # Close the write end of the pipe to unblock the reader thread and trigger it
    # to exit
    os.close(stdout_fileno)
    t.join()

    # Clean up the pipe and restore the original stdout
    os.close(stdout_pipe[0])
    os.dup2(stdout_save, stdout_fileno)
    os.close(stdout_save)

    return create_dict(captured_stdout)

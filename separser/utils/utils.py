import logging
import shutil
import os
import sys
import threading
import subprocess
import requests
try:
    import winreg
except ModuleNotFoundError:
    pass

logger = None


def init_logger(proj_dir, level):
    global logger
    logger = logging.getLogger("StackExchangeParser")
    syslog = logging.FileHandler(filename=proj_dir+'separse.log', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s %(name)s - %(levelname)s:%(message)s')
    syslog.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(syslog)
    return logger


# TODO refactor into class that has various verbosity levels
def log(message, *args, **kwargs):
    global logger
    logger.info(message)
    [logger.debug(arg) for arg in args]
    [logger.debug(kwarg) for kwarg in kwargs]


def find_program_win(program_to_find='SOFTWARE\\7-Zip'):

    try:
        h_key = winreg.CreateKey(winreg.HKEY_LOCAL_MACHINE, program_to_find)
        try:
            prog_path = (winreg.QueryValueEx(h_key, 'Path'))[0]
            return prog_path + '7z'
        except OSError:
            log("7-Zip isn't correctly installed!! ")
            return None
    except PermissionError:
        log("7-Zip not found!! ")
        answer = query_yes_no("Do you wish to install 7zip? ", default='yes')
        if answer:

            url = "https://www.7-zip.org/a/7z1900-x64.msi"
            log("Attempting to download {}".format(url))
            local_filename = url.split('/')[-1]

            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                homepath = os.environ.get('HOMEPATH', ".")
                parentpath = "C:" + homepath + "\\Downloads\\"
                filepath = parentpath + local_filename
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)

            log("Attempting to install {}".format(local_filename))
            print("Please check for a Windows Installer icon on the Task Bar and follow the prompts.")
            subprocess.call('msiexec /i "{fp}" /passive /norestart /l*v {pp}7zip_install.log'.format(fp=filepath,
                                                                                                     pp=parentpath))
            return find_program_win()
        else:
            return None


def find_program_other(cmd='7z'):
    available = shutil.which(cmd=cmd)
    if not available:
        log("7-Zip not found!! ")
        answer = query_yes_no("Do you wish to install 7zip? ", default='yes')
        if answer:
            from pkg_resources import resource_filename
            log("Attempting to download and install 7zip")
            filepath = resource_filename('utils', 'include/install_7zip.sh')
            subprocess.call("bash {}".format(filepath))
        else:
            return None
    else:
        return available


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


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via input() and return answer.

    :param question: is a string that is presented to the user.
    :param default: is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

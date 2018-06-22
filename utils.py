import json
import spur
import traceback
import commands
import requests

# constants
LOGGER = None

def set_logger(logger):
    global LOGGER
    LOGGER = logger

def exec_ssh(host, user, key, ssh_commands):
    command_exe_status = True
    result = None
    shell = spur.SshShell(
        hostname=host,
        username=user,
        private_key_file=key,
        missing_host_key=spur.ssh.MissingHostKey.accept)
    with shell:
        for ssh_command in ssh_commands:
            LOGGER.info('Host - %s: Command - %s', host, ssh_command)
            try:
                result = shell.run(["bash", "-c", ssh_command]).output
            except spur.results.RunProcessError as exception:
                LOGGER.error(ssh_command +" - error: " + traceback.format_exc(exception))
                command_exe_status = False
    return (command_exe_status, result)

def exe_cli(cli_commands):
    command_exe_status = True
    for command in cli_commands:
        res = commands.getstatusoutput(command)
        if res[0] > 0:
            command_exe_status = False
            break

    return command_exe_status

def fill_config(edge_ip, dm_port):
    res = requests.get("http://%s:%d/environment/endpoints" % (edge_ip, dm_port))
    return json.loads(res.text)

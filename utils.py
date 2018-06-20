import spur
import commands

def exec_ssh(host, user, key, ssh_commands):
    shell = spur.SshShell(
        hostname=host,
        username=user,
        private_key_file=key,
        missing_host_key=spur.ssh.MissingHostKey.accept)
    with shell:
        for ssh_command in ssh_commands:
            logging.debug('Host - %s: Command - %s', host, ssh_command)
            try:
                shell.run(["bash", "-c", ssh_command])
            except spur.results.RunProcessError as exception:
                logging.error(
                    ssh_command +
                    " - error: " +
                    traceback.format_exc(exception))

def exe_cli(cli_commands):
    command_exe_status = True
    for command in cli_commands:
        res = commands.getstatusoutput(command)
        if res[0] > 0:
            command_exe_status = False
            break

    return command_exe_status
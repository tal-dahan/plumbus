from engine.orchestrator import Orchestrator
from app.commands import command, CmdContext
from dataclasses import dataclass


@dataclass
class CLISettings:    
    prompt: str
    stop_cmd: str
    orchestrator: Orchestrator


DefaultSettings = CLISettings(
    prompt=">> ",
    stop_cmd="stop",
    orchestrator=None
)


def cli_settings(template: CLISettings = DefaultSettings ,**kwargs):
    s = {**(template.__dict__), **kwargs}
    return CLISettings(**s)


class CLI:
    def __init__(self, settings: CLISettings) -> None:
        self.__settings = settings

    def __get_command(self):
        line = input(self.__settings.prompt)
        parts = line.split(" ")

        return (parts[0], parts[1:])

    def start(self):
        cmd, args = self.__get_command()
        context = CmdContext(self.__settings.orchestrator)

        while cmd != self.__settings.stop_cmd:
            try:
                cmd_callback = command(cmd)
            except:
                print(f"unkown command")
            else:
                try:
                    cmd_callback(args, context)
                except Exception as err:
                    print(f"[!] cmd '{cmd}' raised exception:")
                    print(err)
                    
            cmd, args = self.__get_command()

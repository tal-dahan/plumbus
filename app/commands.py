from engine.orchestrator import Orchestrator
from dataclasses import dataclass


@dataclass
class CmdContext:
    orchestrator: Orchestrator
        

def run_cmd(args, context: CmdContext):
    pipeline, *inputs = args
    data = {i.split("=")[0]: i.split("=")[1] for i in inputs}
    worker_id = context.orchestrator.run(pipeline, data)
    print(f"worker '{worker_id}' has started")


def show_cmd(args, context: CmdContext):
    if args[0] == "workers":
        print("idx.\tid\tpipeline\tstep\tstatus\toutput")

        for idx, worker in enumerate(context.orchestrator.get_workers()):
            print(f"{idx + 1}.\t{worker.id}\t{worker.pipeline.name}\t{worker.current_step + 1}/{len(worker.pipeline.steps)}\t{worker.status.name}\t{worker.output}")
        
    elif args[0] == "pipelines":
        [print(f"[{i + 1}]", r) for i, r in enumerate(context.orchestrator.get_pipelines())]


def stop_cmd(args, context: CmdContext):
    context.orchestrator.stop_worker(args[0])
    print(f"worker '{args[0]}' has stopped")


def remove_cmd(args, context: CmdContext):
    if args[0] == "worker":
        try:
            id_to_remove = int(args[1])
        except:
            raise Exception("invalid id")
        else:
            context.orchestrator.remove_worker(id_to_remove)
            print(f"worker '{args[0]}' has removed")


def print_cmd(args, context: CmdContext):
    print("run, show, stop, remove")


def command(name):
    cmd = None

    if name == "help":
        cmd = print_cmd
    if name == "run":
        cmd = run_cmd
    if name == "show":
        cmd = show_cmd
    if name == "stop":
        cmd = stop_cmd
    if name == "remove":
        cmd = remove_cmd
    
    if not cmd:
        raise Exception('unkown command')
    else:
        return cmd

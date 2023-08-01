from engine.core import Pipeline
from engine.orchestrator import Orchestrator, generate_dummy_pipelines
from app.cli import CLI, cli_settings
import engine.persistence.file_persistence

def main():
    pipes = generate_dummy_pipelines()
    orcha = Orchestrator(pipelines=pipes)
    settings = cli_settings(orchestrator=orcha)
    cli = CLI(settings)
    cli.start()

if __name__ == "__main__":
    main()


from classes.functions import Functions as f
from classes.commands import Commands as commands



if __name__ == '__main__':
    f.log("Starting")
    config = f.import_json('Files/config/config.json')
    runs = f.import_json(config["Runs"])
    for run in runs["Runs"]:
        f.log(run)
        commands.run_command(run, config, f)
    f.log("Done")



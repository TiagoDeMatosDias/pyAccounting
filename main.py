# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from classes.functions import Functions as f
from classes.commands import Commands as commands



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("Starting")
    config = f.import_json('Files/config/config.json')
    runs = f.import_json(config["Runs"])
    for run in runs["Runs"]:
        commands.run_command(run, config, f)


    print("Done")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

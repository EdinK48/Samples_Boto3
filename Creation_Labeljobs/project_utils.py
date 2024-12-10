import os
from typing import List, Tuple

PROJECT_PATH = "<project path>"


def get_projects_from_txt() -> List[str]:
    """
    Retrieve the label projects that need to be processed from the projects.txt file
    
    Args:
        file_path: The path to the text file.

    Returns:
        projects: A list of strings, each representing a line from the rojects.txt file.
    """
    file_path = os.path.join(PROJECT_PATH, "label_project_names.txt")
    with open(file_path, "r") as file:
        lines = file.readlines()
    projects = [line.strip() for line in lines]
    return projects
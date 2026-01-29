import os
from pathlib import Path

repo_name = os.environ["REPO_NAME"]

sonar_file = Path("sonar-project.properties").resolve()

match_line = "sonar.java.binaries=**/*"

template = f"""
sonar.java.libraries=/home/runner/work/{repo_name}/{repo_name}/.m2/**/*.jar
sonar.java.test.binaries=target/test-classes
sonar.java.test.libraries=/home/runner/work/{repo_name}/{repo_name}/.m2/**/*.jar
""".strip("\n")

with open(sonar_file, "r") as f:
    lines = f.readlines()

new_lines = []
inserted = False

for line in lines:
    new_lines.append(line)
    if match_line in line and not inserted:
        new_lines.append(template + "\n")
        inserted = True

if inserted:
    with open(sonar_file, "w") as f:
        f.writelines(new_lines)
    print("Sonar properties updated")
else:
    print("Match line not found. No changes made.")

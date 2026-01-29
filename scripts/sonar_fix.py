from pathlib import Path

# Archivo fijo en la raíz del repo
sonar_file = Path("sonar-project.properties").resolve()

print(f"DEBUG: looking for sonar file at: {sonar_file}")
print(f"DEBUG: file exists? {sonar_file.exists()}")

match_line = "sonar.java.binaries=**/*"

repo_name = Path().resolve().name

template = f"""
sonar.java.libraries=/home/runner/work/{repo_name}/{repo_name}/.m2/**/*.jar
sonar.java.test.binaries=target/test-classes
sonar.java.test.libraries=/home/runner/work/{repo_name}/{repo_name}/.m2/**/*.jar
""".strip("\n")

if not sonar_file.exists():
    print("ERROR: sonar-project.properties no encontrado")
    exit(1)

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

import os
import glob
import subprocess
from itertools import combinations

RESULTS_DIR = './.results'

for i in range(1, 6):
    patron_busqueda = os.path.join(RESULTS_DIR, f'*_Q{i}*.txt')
    files_to_compare = glob.glob(patron_busqueda)

    if len(files_to_compare) < 2:
        continue

    for file1, file2 in combinations(files_to_compare, 2):
        try:
            result = subprocess.run(
                ['diff', '-u', file1, file2], capture_output=True, text=True
            )

            if result.returncode == 1:
                print(
                    '======================================================================='
                )
                print(
                    f"HAY DIFERENCIAS: '{os.path.basename(file1)}' vs '{os.path.basename(file2)}'"
                )
                print(
                    '-----------------------------------------------------------------------'
                )

                print(result.stdout)

        except FileNotFoundError:
            print(
                "Error: El comando 'diff' no se encontró. Asegúrate de que esté instalado."
            )
            break

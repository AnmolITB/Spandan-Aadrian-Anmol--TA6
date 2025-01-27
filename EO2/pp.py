import pandas as pd
import numpy as np
import os


def inspeccionar_fitxer(path):
    """
    Llegeix i mostra les primeres línies d'un fitxer per inspeccionar-ne el contingut.
    """
    try:
        if not os.path.exists(path):
            print(f"El fitxer {path} no existeix.")
            return
        with open(path, 'r') as f:
            print(f"Inspeccionant les primeres 5 línies del fitxer: {path}")
            for _ in range(5):  # Llegeix 5 línies
                print(f.readline().strip())
    except Exception as e:
        print(f"Error al llegir l'arxiu {path}: {e}")


def validar_format(files):
    """
    Valida el format dels fitxers i verifica consistència de dades.
    Retorna un informe detallat sobre errors, valors nuls i inconsistències.
    """
    informes = []

    for file in files:
        if not os.path.exists(file):
            informes.append((file, "Error: El fitxer no existeix"))
            continue

        try:
            # Detecta el delimitador automàticament i llegeix el fitxer
            df = pd.read_csv(file, sep=None, engine='python')
            print(f"\nValidant el fitxer: {file}")

            # Verificar tipus de dades de les columnes
            tipus_dades = df.dtypes
            print(f"Tipus de dades per columna:\n{tipus_dades}")

            # Comptar valors nuls
            valors_nuls = df.isnull().sum()
            print(f"Valors nuls per columna:\n{valors_nuls}")

            # Comptar valors corruptes (p. ex., -999)
            valors_corruptes = (df == -999).sum().to_dict()
            print(f"Valors corruptes (-999) per columna:\n{valors_corruptes}")

            # Verifica si totes les columnes tenen el mateix nombre d'elements
            if len(set(df.apply(len))) != 1:
                print(f"Inconsistència en el nombre de valors per columna al fitxer {file}")

            # Afegir l'informe a la llista de resultats
            informes.append((file, df.shape[1], df.columns.tolist(), valors_nuls.to_dict(), valors_corruptes))

        except Exception as e:
            informes.append((file, None, f"Error: {e}"))

    return informes



fitxer = '../EO1/precip.MIROC5.RCP60.2006-2100.SDSM_REJ'
inspeccionar_fitxer(fitxer)

files = [
    '../EO1/precip.MIROC5.RCP60.2006-2100.SDSM_REJ'
]
resultats = validar_format(files)

# Mostrar els resultats
print("\nResultats de la validació:")
for resultat in resultats:
    print(resultat)

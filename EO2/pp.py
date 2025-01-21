import pandas as pd
import os
import logging

# Configuración del log
logging.basicConfig(filename='dades_analisi.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def inspeccionar_fitxer(path, n_lines=5):
    """
    Llegeix i mostra les primeres línies d'un fitxer per inspeccionar-ne el contingut.
    """
    try:
        if not os.path.exists(path):
            logging.error(f"El fitxer {path} no existeix.")
            return
        with open(path, 'r') as f:
            logging.info(f"Inspeccionant les primeres {n_lines} línies del fitxer: {path}")
            for _ in range(n_lines):
                print(f.readline().strip())
    except FileNotFoundError:
        logging.error(f"Error: El fitxer {path} no existeix.")
    except Exception as e:
        logging.error(f"Error al llegir l'arxiu {path}: {e}")

def calcular_estadistiques(df):
    """
    Calcula estadístiques de les dades processades.
    """
    try:
        # Reemplaçar valors -999 per NaN per facilitar els càlculs
        df.replace(-999, pd.NA, inplace=True)

        # Calcula el percentatge de dades mancants (-999)
        percentatge_dades_mancants = df.isna().mean() * 100

        # Calcula la precipitació total i mitjana per any
        precipitacio_anual = df.groupby('Year')['Precipitation'].agg(['sum', 'mean'])

        # Calcula la tendència de canvi de la precipitació anual
        precipitacio_anual['change_rate'] = precipitacio_anual['sum'].pct_change() * 100

        # Anys més plujosos i més secs
        any_mes_plujos = precipitacio_anual['sum'].idxmax()
        any_mes_sec = precipitacio_anual['sum'].idxmin()

        # Desviació estàndard anual de la precipitació
        precipitacio_anual['std_dev'] = df.groupby('Year')['Precipitation'].std()

        # Nombre de dies de pluja per any (considerant dies amb precipitació > 0)
        dies_pluja_per_any = df[df['Precipitation'] > 0].groupby('Year')['Precipitation'].count()

        # Calcular estadístiques globals
        total_datos = df.shape[0]
        total_dias_sin_registro = df['Precipitation'].isna().sum()
        porcentaje_anual_dias_sin_registro = (total_dias_sin_registro / total_datos) * 100
        promedio_anual_precipitaciones = precipitacio_anual['mean'].mean()

        return {
            'total_datos': total_datos,
            'total_dias_sin_registro': total_dias_sin_registro,
            'porcentaje_anual_dias_sin_registro': porcentaje_anual_dias_sin_registro,
            'promedio_anual_precipitaciones': promedio_anual_precipitaciones
        }

    except Exception as e:
        logging.error(f"Error al calcular estadístiques: {e}")
        return None

def validar_format(files):
    """
    Valida el format dels fitxers i verifica consistència de dades.
    Retorna un informe detallat sobre errors, valors nuls i inconsistències.
    """
    informes = []
    total_files = len(files)
    files_with_mistakes = 0

    for file in files:
        if not os.path.exists(file):
            informes.append((file, "Error: El fitxer no existeix"))
            files_with_mistakes += 1
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
                files_with_mistakes += 1

            # Verificar si el fitxer té 12 mesos en ordre
            expected_months = list(range(1, 13))
            actual_months = df['Month'].unique().tolist()
            if sorted(actual_months) != expected_months:
                print(f"Error: El fitxer {file} no té els 12 mesos en ordre correcte")
                informes.append((file, "Error: Els mesos no estan en ordre correcte"))
                files_with_mistakes += 1
            else:
                # Calcular estadístiques
                estadistiques = calcular_estadistiques(df)
                informes.append((file, estadistiques))

        except Exception as e:
            informes.append((file, None, f"Error: {e}"))
            files_with_mistakes += 1

    processing_percent = (total_files - files_with_mistakes) / total_files * 100

    return informes, total_files, files_with_mistakes, processing_percent

def processar_carpeta(folder_path):
    """
    Processa tots els fitxers d'una carpeta.
    """
    files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.dat')]
    resultats, total_files, files_with_mistakes, processing_percent = validar_format(files)

    if total_files == 0:
        print("No se encontraron archivos para procesar.")
        return

    # Calcular estadísticas globales
    total_datos = sum(r[1]['total_datos'] for r in resultats if r[1] is not None)
    total_dias_sin_registro = sum(r[1]['total_dias_sin_registro'] for r in resultats if r[1] is not None)
    porcentaje_anual_dias_sin_registro = sum(r[1]['porcentaje_anual_dias_sin_registro'] for r in resultats if r[1] is not None) / total_files
    promedio_anual_precipitaciones = sum(r[1]['promedio_anual_precipitaciones'] for r in resultats if r[1] is not None) / total_files

    # Escribir las estadísticas globales en el archivo datos.txt
    with open('datos.txt', 'w') as f:
        f.write(f"Total datos: {total_datos}\n")
        f.write(f"Total días sin registro: {total_dias_sin_registro}\n")
        f.write(f"Porcentaje anual de días sin registro: {porcentaje_anual_dias_sin_registro:.2f}%\n")
        f.write(f"Promedio anual de precipitaciones: {promedio_anual_precipitaciones:.2f}\n")

    # Mostrar estadísticas globales en pantalla
    print(f"\nTotal datos: {total_datos}")
    print(f"Total días sin registro: {total_dias_sin_registro}")
    print(f"Porcentaje anual de días sin registro: {porcentaje_anual_dias_sin_registro:.2f}%")
    print(f"Promedio anual de precipitaciones: {promedio_anual_precipitaciones:.2f}")

# Especificar la carpeta a processar
folder_path = '../EO1/precip.MIROC5.RCP60.2006-2100.SDSM_REJ'
processar_carpeta(folder_path)
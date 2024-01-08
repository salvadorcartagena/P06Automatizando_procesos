import requests
import pandas as pd
import io
import sys

def limpieza_categorizacion_datos(dataframe):
    """
    Realiza la limpieza y categorización de los datos en el DataFrame.

    Parameters:
    - dataframe: DataFrame de pandas con los datos a procesar.

    Returns:
    - DataFrame procesado.
    """
    # Verificar valores faltantes
    assert dataframe.isnull().sum().sum() == 0, "¡Hay valores faltantes en el DataFrame!"

    # Verificar filas duplicadas
    duplicados = dataframe.duplicated().sum()
    if duplicados > 0:
        print(f"Hay {duplicados} filas duplicadas en el DataFrame.")
        dataframe.drop_duplicates(inplace=True)
    else:
        print("No hay filas duplicadas en el DataFrame.")

    # Crea una columna de categorización por las edades
    dataframe['categoria_edad'] = pd.cut(
        dataframe['age'],
        bins=[0, 12, 19, 39, 59, float('inf')],
        labels=['Niño', 'Adolescente', 'Jóvenes adulto', 'Adulto', 'Adulto mayor']
    )

    return dataframe

def procesar_datos(url):
    """
    Descarga datos desde la URL, realiza el procesamiento y guarda el resultado en un archivo CSV.

    Parameters:
    - url: URL que apunta al conjunto de datos a procesar.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanza una excepción para códigos de estado no exitosos

        datos = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        datos_procesados = limpieza_categorizacion_datos(datos)

        archivo_salida = 'heart_failure_dataset_procesado.csv'
        datos_procesados.to_csv(archivo_salida, index=False)
        print(f"Información procesada y guardada en '{archivo_salida}'")

    except requests.exceptions.RequestException as req_ex:
        print(f"Error en la solicitud HTTP: {str(req_ex)}")
    except pd.errors.EmptyDataError:
        print("Error: El conjunto de datos está vacío.")
    except Exception as e:
        print(f"Error desconocido: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Proporcione la URL para ejecutar el código.")
    else:
        url_datos = sys.argv[1]
        procesar_datos(url_datos)

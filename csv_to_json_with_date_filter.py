import pandas as pd

# Leer CSV
df = pd.read_csv('flights_sample_3m.csv')

# Convertir FL_DATE a datetime
df['FL_DATE'] = pd.to_datetime(df['FL_DATE'], errors='coerce')

# Filtrar solo del 1 al 15 de abril de 2020
fecha_inicio = pd.Timestamp('2020-04-01')
fecha_fin = pd.Timestamp('2020-04-15')
df_filtrado = df[(df['FL_DATE'] >= fecha_inicio) & (df['FL_DATE'] <= fecha_fin)]

# Convertir FL_DATE a formato YYYY-MM-DD legible
df_filtrado['FL_DATE'] = df_filtrado['FL_DATE'].dt.strftime('%Y-%m-%d')

# Exportar a JSON
df_filtrado.to_json('flights_sample_first_half_april_2020.json', orient='records', indent=4, force_ascii=False)

print(f"¡Conversión completada! {len(df_filtrado)} registros del 1 al 15 de abril de 2020 guardados en JSON.")

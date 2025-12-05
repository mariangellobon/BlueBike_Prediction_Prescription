"""
Script para crear stock CSV a partir de test_final.csv

Para cada día, hora y estación (loc_id), calcula el stock (cantidad de bicis al inicio de la hora).
Inicializa el stock y luego lo actualiza hora a hora sumando net_demand.
Encuentra el valor óptimo de inicialización que minimiza la necesidad de truncar
(mantener el stock dentro del rango de capacidad de la estación).
"""

import pandas as pd
import numpy as np
from tqdm import tqdm


def load_data(test_csv_path, capacities_csv_path):
    """Carga los datos de test y capacidades"""
    print("Cargando datos...")
    df_test = pd.read_csv(test_csv_path)
    df_capacities = pd.read_csv(capacities_csv_path)
    
    # Crear diccionario de capacidades por loc_id
    capacity_dict = dict(zip(df_capacities['loc_id'], df_capacities['Total Docks']))
    
    # Agregar capacidad a df_test
    df_test['capacity'] = df_test['loc_id'].map(capacity_dict)
    
    # Ordenar por día, hora y loc_id para procesamiento secuencial
    df_test = df_test.sort_values(['day', 'hour', 'loc_id']).reset_index(drop=True)
    
    print(f"Datos cargados: {len(df_test)} filas")
    print(f"Estaciones únicas: {df_test['loc_id'].nunique()}")
    print(f"Días únicos: {df_test['day'].nunique()}")
    
    return df_test, capacity_dict


def calculate_stock_for_initial_value(df_day_station, initial_stock, capacity):
    """
    Calcula el stock para un día y estación dado un valor inicial.
    
    El stock en cada hora representa la cantidad de bicis al INICIO de esa hora.
    Luego se actualiza sumando net_demand y truncando si es necesario.
    
    Retorna el stock en cada hora y el número de truncaciones necesarias.
    """
    hours = sorted(df_day_station['hour'].unique())
    stock_by_hour = {}
    truncations = 0
    
    # Stock al inicio de la primera hora (hour 0)
    current_stock = initial_stock
    
    for hour in hours:
        # El stock al inicio de esta hora
        stock_by_hour[hour] = current_stock
        
        # Obtener net_demand para esta hora
        row = df_day_station[df_day_station['hour'] == hour].iloc[0]
        net_demand = row['net_demand']
        
        # Calcular stock al final de esta hora (que será el stock al inicio de la siguiente)
        new_stock = current_stock + net_demand
        
        # Truncar si es necesario y contar truncaciones
        if new_stock < 0:
            truncations += abs(new_stock)  # Contar cuánto se truncó
            new_stock = 0
        elif new_stock > capacity:
            truncations += (new_stock - capacity)  # Contar cuánto se truncó
            new_stock = capacity
        
        # El stock al inicio de la siguiente hora será este nuevo stock
        current_stock = new_stock
    
    return stock_by_hour, truncations


def find_optimal_initial_stock(df_day_station, capacity):
    """
    Encuentra el valor inicial óptimo (0 a capacity) que minimiza las truncaciones.
    Prueba todas las posibilidades de 0 a capacity (inclusive).
    
    Retorna:
    - best_initial_stock: El stock inicial óptimo
    - best_stock_by_hour: Diccionario con stock por hora
    - min_truncations: Número mínimo de truncaciones encontrado
    """
    best_initial_stock = 0
    min_truncations = float('inf')
    best_stock_by_hour = None
    
    # Probar todos los valores posibles de 0 a capacity (inclusive)
    capacity_int = int(capacity)
    for initial_stock in range(capacity_int + 1):
        stock_by_hour, truncations = calculate_stock_for_initial_value(
            df_day_station, initial_stock, capacity
        )
        
        if truncations < min_truncations:
            min_truncations = truncations
            best_initial_stock = initial_stock
            best_stock_by_hour = stock_by_hour
    
    return best_initial_stock, best_stock_by_hour, min_truncations


def create_stock_csv(df_test, capacity_dict):
    """
    Crea el CSV de stock calculando el stock óptimo para cada día, hora y estación.
    """
    print("\nCalculando stock para cada día, estación y hora...")
    
    # Obtener días únicos
    unique_days = sorted(df_test['day'].unique())
    unique_stations = sorted(df_test['loc_id'].unique())
    
    results = []
    total_combinations = len(unique_days) * len(unique_stations)
    
    with tqdm(total=total_combinations, desc="Procesando") as pbar:
        for day in unique_days:
            df_day = df_test[df_test['day'] == day]
            
            for loc_id in unique_stations:
                df_day_station = df_day[df_day['loc_id'] == loc_id]
                
                if len(df_day_station) == 0:
                    pbar.update(1)
                    continue
                
                capacity = capacity_dict.get(loc_id)
                if capacity is None or pd.isna(capacity):
                    pbar.update(1)
                    continue
                
                # Encontrar stock inicial óptimo
                optimal_initial, stock_by_hour, truncations = find_optimal_initial_stock(
                    df_day_station, capacity
                )
                
                # Agregar resultados para cada hora
                for hour, stock in stock_by_hour.items():
                    row = df_day_station[df_day_station['hour'] == hour].iloc[0].copy()
                    row['stock'] = stock
                    row['initial_stock'] = optimal_initial
                    row['total_truncations'] = truncations  # Total para todo el día
                    results.append(row)
                
                pbar.update(1)
    
    # Crear DataFrame con resultados
    df_stock = pd.DataFrame(results)
    
    # Seleccionar columnas relevantes para el output
    output_columns = ['day', 'hour', 'loc_id', 'name', 'stock', 'net_demand', 
                      'capacity', 'latitude', 'longitude']
    
    # Asegurar que todas las columnas existan
    available_columns = [col for col in output_columns if col in df_stock.columns]
    df_stock_output = df_stock[available_columns].copy()
    
    # Ordenar por día, hora y loc_id
    df_stock_output = df_stock_output.sort_values(['day', 'hour', 'loc_id']).reset_index(drop=True)
    
    return df_stock_output, df_stock


def main():
    """Función principal"""
    # Rutas de archivos
    test_csv_path = 'test_final.csv'
    capacities_csv_path = 'capacities.csv'
    output_csv_path = 'stock.csv'
    
    # Cargar datos
    df_test, capacity_dict = load_data(test_csv_path, capacities_csv_path)
    
    # Crear stock CSV
    df_stock_output, df_stock_full = create_stock_csv(df_test, capacity_dict)
    
    # Guardar resultados
    print(f"\nGuardando resultados en {output_csv_path}...")
    df_stock_output.to_csv(output_csv_path, index=False)
    print(f"Stock CSV creado exitosamente: {len(df_stock_output)} filas")
    
    # Estadísticas
    print("\n=== Estadísticas ===")
    print(f"Total de filas en stock CSV: {len(df_stock_output)}")
    print(f"Días procesados: {df_stock_output['day'].nunique()}")
    print(f"Estaciones procesadas: {df_stock_output['loc_id'].nunique()}")
    print(f"Rango de stock: [{df_stock_output['stock'].min():.2f}, {df_stock_output['stock'].max():.2f}]")
    
    if 'total_truncations' in df_stock_full.columns:
        total_truncations = df_stock_full.groupby(['day', 'loc_id'])['total_truncations'].first().sum()
        print(f"Total de truncaciones acumuladas: {total_truncations:.2f}")
    
    # Guardar también versión completa con información de inicialización
    output_full_path = 'stock_full.csv'
    df_stock_full.to_csv(output_full_path, index=False)
    print(f"\nVersión completa guardada en {output_full_path}")


if __name__ == '__main__':
    main()


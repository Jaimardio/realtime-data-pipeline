import os
import duckdb

def query_results():
    print("🔍 Odpytanie zapisanych plików Parquet za pomocą DuckDB (SQL):\n")
    
    action_path = "analytics_output/action_summary.parquet"
    user_path = "analytics_output/user_activity.parquet"
    
    if not os.path.exists(action_path):
        print(f"⚠️ Nie znaleziono plików analitycznych. Uruchom najpierw 'analytics.py'!")
        return

    # 1. Tabela: Podsumowanie akcji
    print("=" * 50)
    print("📊 1. Podsumowanie zdarzeń według typu akcji:")
    print("=" * 50)
    query_actions = f"""
        SELECT 
            Akcja, 
            Liczba_Zdarzen,
            ROUND(Liczba_Zdarzen * 100.0 / (SELECT SUM(Liczba_Zdarzen) FROM '{action_path}'), 2) AS udzial_procentowy
        FROM '{action_path}'
        ORDER BY Liczba_Zdarzen DESC
    """
    print(duckdb.sql(query_actions).df().to_string(index=False))
    
    # 2. Tabela: Aktywność użytkowników
    if os.path.exists(user_path):
        print("\n" + "=" * 50)
        print("👤 2. Top aktywnych użytkowników (z 'user_activity.parquet'):")
        print("=" * 50)
        query_users = f"""
            SELECT 
                Uzytkownik, 
                Liczba_Akcji
            FROM '{user_path}'
            ORDER BY Liczba_Akcji DESC
            LIMIT 5
        """
        print(duckdb.sql(query_users).df().to_string(index=False))
        
    print("\n" + "=" * 50)
    print("✅ Zapytania SQL wykonane pomyślnie przez DuckDB!")

if __name__ == "__main__":
    query_results()
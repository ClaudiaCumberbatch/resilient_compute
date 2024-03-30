import sqlite3
import re

def process_data():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    
    table_name="overhead"
    c.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} (run_id TEXT, time_diff float)''')

    with open('nohup.out', 'r') as file:
        data = file.read()

    pattern = re.compile(r"\('([0-9a-f\-]+)'.*?\) run ([0-9.]+) ms\n(\d+)")
    matches = pattern.findall(data)

    for match in matches:
        id_str, time_a_str, time_b_str = match
        time_a = float(time_a_str)
        time_b = float(time_b_str)
        time_difference = time_b - time_a
        print(f"{id_str}: {time_difference}")
        c.execute('INSERT INTO overhead (run_id, time_diff) VALUES (?, ?)', (id_str, time_difference))

    conn.commit()

    conn.close()

    print(f"Processed {len(matches)} entries.")

if __name__ == "__main__":
    process_data()


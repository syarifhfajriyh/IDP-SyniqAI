import psycopg2

conn = psycopg2.connect(
    host='192.168.0.11',
    port=5432,
    dbname='postgres',
    user='postgres',
    password='password'
)

cur = conn.cursor()
cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
tables = cur.fetchall()

print('\n📋 Available tables in PostgreSQL (192.168.0.11):')
print('='*50)
for t in tables:
    print(f'  • {t[0]}')
print('='*50)

conn.close()

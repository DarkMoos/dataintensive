from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor
import time

def increment_likes(uri, user, password, count):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    def increment(tx):
        tx.run("MATCH (item:Item {name: 'Item1'}) SET item.likes = item.likes + 1")

    with driver.session() as session:
        for _ in range(count):
            session.write_transaction(increment)

    driver.close()

if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "superadmin"

    start_time = time.time()

    # Запустимо 10 клієнтів, кожен збільшує лічильник 10_000 разів
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(increment_likes, uri, user, password, 10000) for _ in range(10)]

    end_time = time.time()
    print("Інкрементація завершена!")
    print(f"Час виконання: {end_time - start_time} секунд")
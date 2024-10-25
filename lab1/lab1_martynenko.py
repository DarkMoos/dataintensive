import hazelcast
from threading import Thread, Lock
import time

# Підключення до кластера Hazelcast
client = hazelcast.HazelcastClient(cluster_name="lab1")
dist_map = client.get_map("distributed-counter").blocking()

# Глобальна змінна для лічильника
global_counter = 0

def increment_with_lock(key):
    """Функція для інкрементації з локальним блокуванням."""
    global global_counter
    lock = Lock()
    for _ in range(10000):
        with lock:
            global_counter += 1
        dist_map.put(key, global_counter)

def simple_increment(key):
    """Функція для інкрементації без блокування."""
    dist_map.put(key, 0)
    for _ in range(10000):
        current_value = dist_map.get(key)
        dist_map.put(key, current_value + 1)

def pessimistic_increment(key):
    """Функція для інкрементації з песимістичним блокуванням."""
    if not dist_map.contains_key(key):
        dist_map.put(key, 0)
    for _ in range(10000):
        dist_map.lock(key)
        try:
            current_value = dist_map.get(key)
            dist_map.put(key, current_value + 1)
        finally:
            dist_map.unlock(key)

def optimistic_increment(key):
    """Функція для інкрементації з оптимістичним блокуванням."""
    if not dist_map.contains_key(key):
        dist_map.put(key, 0)
    for _ in range(10000):
        while True:
            current_value = dist_map.get(key)
            new_value = current_value + 1
            if dist_map.replace_if_same(key, current_value, new_value):
                break

def atomic_increment_with_cp(key):
    """Функція для інкрементації з використанням атомарного лічильника в CP Subsystem."""
    atomic_long = client.cp_subsystem.get_atomic_long(key).blocking()
    for _ in range(10000):
        atomic_long.add_and_get(1)

def execute_task(task_function, task_name):
    """Функція для виконання завдання в багатопоточному режимі."""
    print(f"--- Виконання завдання: {task_name} ---")
    start_time = time.time()
    threads = [Thread(target=task_function, args=(task_name,)) for _ in range(10)]
    
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    elapsed_time = time.time() - start_time
    if task_name == "cp_atomic_increment":
        final_value = client.cp_subsystem.get_atomic_long(task_name).blocking().get()
    else:
        final_value = dist_map.get(task_name)
    print(f"Час виконання: {elapsed_time:.4f} секунд")
    print(f"Фінальне значення: {final_value}")

def reset_atomic_counter_with_cp(key):
    """Скидання атомарного лічильника для CP Subsystem."""
    atomic_long = client.cp_subsystem.get_atomic_long(key).blocking()
    atomic_long.set(0)

if __name__ == "__main__":
    # Виконання завдань з Distributed Map
    execute_task(increment_with_lock, "lock_increment")
    execute_task(simple_increment, "simple_increment")
    execute_task(pessimistic_increment, "pessimistic_increment")
    execute_task(optimistic_increment, "optimistic_increment")
    
    # Скидання значення лічильника перед запуском завдання з CP Subsystem
    reset_atomic_counter_with_cp("cp_atomic_increment")
    
    # Виконання завдання з підтримкою CP Subsystem
    execute_task(atomic_increment_with_cp, "cp_atomic_increment")

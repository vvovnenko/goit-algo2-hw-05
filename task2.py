import json
import time
from typing import Optional
from hyperloglog import HyperLogLog
import pandas as pd


def extract_ip_from_log(line: str) -> Optional[str]:
    try:
        log_entry = json.loads(line)
        return log_entry.get("remote_addr")
    except (json.JSONDecodeError, KeyError):
        return None


def create_ip_stream(file_path: str) -> list[str]:
    with open(file_path, "r") as log_file:
        for line in log_file:
            ip = extract_ip_from_log(line)
            if ip:
                yield ip


def count_unique_ips_exact(ips: list[str]) -> int:
    unique_ips = set()
    for ip in ips:
        unique_ips.add(ip)

    return len(unique_ips)


def count_unique_ips_hll(ips: list[str], error_rate: float = 0.01) -> float:
    hll = HyperLogLog(error_rate)
    for ip in ips:
        hll.add(ip)

    return hll.card()


def compare_methods(file_path: str) -> None:
    # Точний підрахунок
    print("Точний підрахунок унікальних IP-адрес...")
    start_exact = time.time()
    exact_count = count_unique_ips_exact(create_ip_stream(file_path))
    exact_time = time.time() - start_exact

    # HyperLogLog підрахунок
    print("Підрахунок за допомогою HyperLogLog...")
    start_hll = time.time()
    hll_count = count_unique_ips_hll(create_ip_stream(file_path))
    hll_time = time.time() - start_hll

    # Порівняння результатів
    results = pd.DataFrame(
        {
            "Метод": ["Точний підрахунок", "HyperLogLog"],
            "Унікальні елементи": [exact_count, hll_count],
            "Час виконання (сек.)": [exact_time, hll_time],
        }
    )

    print("Результати порівняння:")
    print(results)


if __name__ == "__main__":
    log_file_path = "lms-stage-access.log"
    compare_methods(log_file_path)

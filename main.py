# -*- coding: utf-8 -*-
"""
Технiчний опис завдання
Розробіть програму, яка паралельно обробляє та аналізує текстові файли для пошуку визначених ключових слів.
Створіть дві версії програми: одну — з використанням модуля threading для багатопотокового програмування,
та іншу — з використанням модуля multiprocessing для багатопроцесорного програмування.
"""
from multiprocessing import Queue, Process, Semaphore, Manager
import threading
import timeit
import logging
import os

FOLDER_NAME = "data"
REAL_PATTERN = "алгоритмів"
ALL_FOLDERS = []
ATTEMPTS = 1
RESULTS = {}
MAX_PROCESSES = MAX_THREADS_COUNT = 3


def get_all_files(directory, extension='.txt'):
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(extension):
                file_list.append(os.path.abspath(os.path.join(root, file)))
    return file_list


def read_file(filename, encoding='cp1251'):
    try:
        with open(filename, 'r', encoding=encoding) as f:
            return f.read()
    except Exception as e:
        logging.error(f'Error reading file {filename}: {e}')
        return ''


def build_shift_table(pattern):
    table = {}
    length = len(pattern)
    for index, char in enumerate(pattern[:-1]):
        table[char] = length - index - 1
    table.setdefault(pattern[-1], length)
    return table


def boyer_moore_search(path: str, pattern: str):
    text = read_file(path)
    shift_table = build_shift_table(pattern)
    i = 0

    while i <= len(text) - len(pattern):
        j = len(pattern) - 1
        while j >= 0 and text[i + j] == pattern[j]:
            j -= 1
        if j < 0:
            return i
        i += shift_table.get(text[i + len(pattern) - 1], len(pattern))
    return -1


def worker(semaphore: Semaphore, queue: Queue, path: str, pattern: str = REAL_PATTERN):
    logging.info('Starting worker with path {}'.format(path))
    with semaphore:
        index = boyer_moore_search(path, pattern)
        if index != -1:
            logging.info(f'Pattern found in file: {path}, index: {index}')
            queue.put(path)
        else:
            logging.info('Pattern found in file: {path}')
        logging.info('Exiting')


def collect_results(queue: Queue, pattern: str = REAL_PATTERN):
    result = {}
    while not queue.empty():
        path = queue.get()
        if REAL_PATTERN not in result:
            result[REAL_PATTERN] = []
        result[REAL_PATTERN].append(path)
    return result


def multithreading_execution(all_files: list):
    pool = threading.Semaphore(MAX_PROCESSES)
    queue = Queue()
    threads = [threading.Thread(target=worker, args=(pool, queue, path, REAL_PATTERN)) for path in all_files]
    [th.start() for th in threads]
    [th.join() for th in threads]
    print(collect_results(queue))


def multiprocessing_execution(all_files: list):
    queue = Queue()
    pool = Semaphore(MAX_PROCESSES)
    with Manager() as manager:
        _data = manager.dict()
        processes = [Process(target=worker, args=(pool, queue, path, REAL_PATTERN)) for path in all_files]
        [p.start() for p in processes]
        [p.join() for p in processes]
    print(collect_results(queue))


if __name__ == '__main__':
    format = "%(threadName)s %(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    all_files = get_all_files(FOLDER_NAME)
    multithreading = timeit.timeit(lambda: multithreading_execution(all_files), number=ATTEMPTS)
    multiprocessing = timeit.timeit(lambda: multiprocessing_execution(all_files), number=ATTEMPTS)
    print(f'Time for multiprocessing: {multiprocessing}, time for multithreading: {multithreading}')

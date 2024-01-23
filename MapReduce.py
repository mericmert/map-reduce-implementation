import json
import time
from abc import ABC, abstractmethod
from multiprocessing import Process
import zmq
from datetime import datetime
class MapReduce(ABC):

    PRODUCER_ENDPOINT = "tcp://127.0.0.1:5555"
    CONSUMER_ENDPOINT = "tcp://127.0.0.1:5555"
    COLLECTION_ENDPOINT = "tcp://127.0.0.1:5558"

    def __init__(self, num_workers: int):
        self.num_workers = num_workers

    @abstractmethod
    def map(self):
        pass

    @abstractmethod
    def reduce(self):
        pass

    def _producer(self, data_list):
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.bind(MapReduce.PRODUCER_ENDPOINT)
        chunk_size = len(data_list) // self.num_workers
        extra_items = len(data_list) % self.num_workers

        start_idx = 0
        for i in range(self.num_workers):
            end_idx = start_idx + chunk_size + (1 if i < extra_items else 0)
            chunk = data_list[start_idx:end_idx]
            socket.send_json(chunk)
            time.sleep(0.1)
            start_idx = end_idx
    def _consumer(self):
        context = zmq.Context()
        receiver_socket = context.socket(zmq.PULL)
        receiver_socket.connect(MapReduce.CONSUMER_ENDPOINT)
        data_chunk = receiver_socket.recv_json()
        partial_result = self.map(data_chunk)
        if not isinstance(partial_result, dict):
            raise ValueError("Map method must return a dictionary!")

        sender_socket = context.socket(zmq.PUSH)
        sender_socket.connect(MapReduce.COLLECTION_ENDPOINT)
        sender_socket.send_json(partial_result)


    def _result_collector(self):
        context = zmq.Context()
        receiver_socket = context.socket(zmq.PULL)
        receiver_socket.bind(MapReduce.COLLECTION_ENDPOINT)

        partial_results = []
        for _ in range(self.num_workers):
            result = receiver_socket.recv_json()
            partial_results.append(result)
        final_result = self.reduce(partial_results)
        with open(f"results-{datetime.now().strftime('%s')}.txt", 'w') as file:
            json.dump(final_result, file)


    def start(self, file_name: str):
        with open(file_name, "r") as file:
            data_list = [line.strip('\n') for line in file.readlines()]

        producer = Process(target=self._producer, args=(data_list,))
        producer.start()

        consumer_processes = []
        for _ in range(self.num_workers):
            consumer = Process(target=self._consumer)
            consumer.start()
            consumer_processes.append(consumer)

        result_collector = Process(target=self._result_collector)
        result_collector.start()

        producer.join()
        for consumer in consumer_processes:
            consumer.join()
        result_collector.join()


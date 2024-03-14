import json
from logging import Logger
from typing import List, Dict
from datetime import datetime

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 100

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        load_dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        load_src = 'stg-service-orders'

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg['payload']

            order_id = order['id']
            order_dt = order['date']
            order_cost = order['cost']
            order_payment = order['payment']
            order_status = order['status']

            user_id = order["user"]["id"]
            user_name = order["user"]["name"]
            user_login = order["user"]["login"]

            restaurant_id = order['restaurant']['id']
            restaurant_name = order['restaurant']['name']

            self._dds_repository.h_user_insert(user_id, load_dt, load_src)

            self._dds_repository.h_restaurant_insert(restaurant_id, load_dt, load_src) 

            self._dds_repository.h_order_insert(order_id, order_dt, load_dt, load_src) 

            products = order['products']
            
            for product in products:
                product_id = product["id"]
                product_name = product["name"]
                category_name = product["category"]

                self._dds_repository.h_product_insert(product_id, product_name, load_dt, load_src)

                self._dds_repository.h_category_insert(category_name, product_name, load_dt, load_src)


            self._dds_repository.l_order_product_insert(order_id, product_id, load_dt, load_src)

            self._dds_repository.l_product_restaurant_insert(product_id, restaurant_id, load_dt, load_src)

            self._dds_repository.l_product_category_insert(product_id, category_name, load_dt, load_src)

            self._dds_repository.l_order_user_insert(order_id, user_id, load_dt, load_src)
            
            self._dds_repository.s_user_names_insert(user_id, user_name, user_login, load_dt, load_src)

            self._dds_repository.s_product_names_insert(product_id, product_name, load_dt, load_src)

            self._dds_repository.s_restaurant_names_insert(restaurant_id, restaurant_name, load_dt, load_src)

            self._dds_repository.s_order_cost_insert(order_id, order_cost, order_payment, restaurant_name, load_dt, load_src)

            self._dds_repository.s_order_status_insert(order_id, order_status, load_dt, load_src)

            self._producer.produce(msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
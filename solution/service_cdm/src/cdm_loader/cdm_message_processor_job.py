from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg['payload']
            user_id = order["user"]["id"]

            products = order['products']

            for product in products:
                product_id = product["id"]
                product_name = product["name"]
                category_id = product["id"]
                category_name = product["category"]

            self._cdm_repository.user_product_counters_insert(user_id, product_id, product_name)

            self._cdm_repository.user_category_counters_insert(user_id, category_id, category_name)


        self._logger.info(f"{datetime.utcnow()}: FINISH")

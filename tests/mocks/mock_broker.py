from collections import defaultdict
from typing import Callable, Dict, List, Tuple, Any
from unittest.mock import MagicMock
import logging

from src.messaging.broker import Broker

logger = logging.getLogger(__name__)


class MockChannel:
    def __init__(self):
        self.basic_ack = MagicMock()
        self.basic_nack = MagicMock()
        self.is_open = True


class MockMethod:
    def __init__(self, delivery_tag=1):
        self.delivery_tag = delivery_tag


class MockBroker(Broker):
    def __init__(self):
        logger.debug('Initializing MockBroker')
        self.queues: Dict[str, List[bytes]] = defaultdict(list)
        self.consumers: Dict[str, Callable] = {}
        self.published_messages: List[Tuple[str, str, bytes]] = []

        self.call_log = defaultdict(list)

    def exchange_declare(self, exchange_name: str, exchange_type: str):
        self.call_log['exchange_declare'].append(
            {'exchange_name': exchange_name, 'exchange_type': exchange_type}
        )
        logger.debug(
            f'MockBroker: exchange_declare called: Name={exchange_name}, Type={exchange_type}'
        )

    def queue_declare(
        self, queue_name: str = '', exclusive: bool = False, durable: bool = True
    ) -> str:
        self.call_log['queue_declare'].append(
            {'queue_name': queue_name, 'exclusive': exclusive, 'durable': durable}
        )
        if not queue_name:
            queue_name = f'amq.gen-{hash(MagicMock())}'
        if queue_name not in self.queues:
            self.queues[queue_name] = []
        logger.debug(
            f"MockBroker: Queue declared: Name='{queue_name}', Exclusive={exclusive}, Durable={durable}"
        )
        return queue_name

    def queue_bind(self, exchange_name: str, queue_name: str):
        self.call_log['queue_bind'].append(
            {'exchange_name': exchange_name, 'queue_name': queue_name}
        )
        logger.debug(
            f'MockBroker: queue_bind called: Exchange={exchange_name}, Queue={queue_name}'
        )

    def put(
        self,
        exchange: str = '',
        routing_key: str = '',
        body: bytes = b'',
        properties: Any = None,
    ):
        self.call_log['put'].append(
            {
                'exchange': exchange,
                'routing_key': routing_key,
                'body_len': len(body),
                'properties': properties,
            }
        )
        logger.debug(
            f"MockBroker: put called. Exchange='{exchange}', RoutingKey='{routing_key}', BodyLen={len(body)}"
        )
        self.published_messages.append((exchange, routing_key, body))

        if not exchange and routing_key in self.queues:
            logger.debug(
                f"MockBroker: Message added to simulated queue: '{routing_key}'"
            )
            self.queues[routing_key].append(body)
        elif exchange:
            logger.debug(
                f"MockBroker: Message published to exchange '{exchange}' (routing not simulated here)"
            )

    def start_consuming(self, callback: Callable, queue_name: str):
        self.call_log['consume'].append(
            {'queue_name': queue_name, 'callback': callback}
        )
        logger.debug(f"MockBroker: Registering consumer for queue: '{queue_name}'")
        if queue_name not in self.queues:
            logger.error(
                f"MockBroker: Attempted to consume from non-existent queue '{queue_name}'"
            )
            raise ValueError(
                f"MockBroker: Queue '{queue_name}' does not exist for consumption."
            )
        self.consumers[queue_name] = callback

    def close(self):
        self.call_log['close'].append({})
        logger.debug('MockBroker: close called')

    def get_published_messages(self, routing_key: str) -> List[bytes]:
        return [
            body
            for ex, rk, body in self.published_messages
            if not ex and rk == routing_key
        ]

    def simulate_message(
        self, queue_name: str, message_body: bytes, delivery_tag: int = 1
    ):
        if queue_name in self.consumers:
            callback = self.consumers[queue_name]
            mock_channel = MockChannel()
            mock_method = MockMethod(delivery_tag=delivery_tag)
            logger.debug(
                f"MockBroker: Simulating message to queue '{queue_name}' (Tag: {delivery_tag})"
            )

            callback(mock_channel, mock_method, None, message_body)
            return mock_channel
        else:
            logger.error(
                f"MockBroker: No consumer registered for queue '{queue_name}' in simulate_message"
            )
            raise ValueError(
                f"MockBroker: No consumer registered for queue '{queue_name}'"
            )

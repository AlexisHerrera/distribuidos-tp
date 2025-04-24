import logging
from typing import Dict, Type

from src.messaging.protocol.message import Message
from src.server.base_node import BaseNode
from src.server.sinks.base_sink_logic import BaseSinkLogic
from src.server.sinks.q1_arg_spa_2000_sink_logic import Q1ArgSpa2000
from src.server.sinks.q2_top5_budget_sink_logic import Q2Top5BudgetSinkLogic
from src.server.sinks.q3_max_min_avg_rating import Q3MaxMinAvgRatingSinkLogic
from src.server.sinks.q4_top10_actors_sink_logic import Q4Top10ActorsSinkLogic
from src.server.sinks.q5_sentiment_avg_budget_revenue_sink_logic import (
    Q5SentimentAvgBudgetRevenueSinkLogic,
)
from src.utils.config import Config

logger = logging.getLogger(__name__)

AVAILABLE_SINK_LOGICS = {
    'q1': Q1ArgSpa2000,
    'q2': Q2Top5BudgetSinkLogic,
    'q3': Q3MaxMinAvgRatingSinkLogic,
    'q4': Q4Top10ActorsSinkLogic,
    'q5': Q5SentimentAvgBudgetRevenueSinkLogic,
}


class SinkNode(BaseNode):
    def __init__(self, config: Config, sink_type: str):
        super().__init__(config, sink_type)
        self.logic: BaseSinkLogic
        logger.info(f"SinkNode '{sink_type}' initialized.")
        self.should_send_results_before_eof = True

    def _get_logic_registry(self) -> Dict[str, Type]:
        return AVAILABLE_SINK_LOGICS

    def handle_message(self, message: Message):
        try:
            self.logic.merge_results(message)
        except Exception as e:
            logger.error(f'Error processing message in SinkNode: {e}', exc_info=True)


if __name__ == '__main__':
    SinkNode.main(AVAILABLE_SINK_LOGICS)

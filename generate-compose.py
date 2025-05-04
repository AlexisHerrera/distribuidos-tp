import argparse

import yaml

DOCKER_COMPOSE_FILENAME = 'docker-compose.yaml'
NETWORK_NAME = 'testing_net'
BASE_PORT = 6000
SMALL_DATASET_PATH = './.data-small'
DATASET_PATH = './.data'
RESULTS_PATH = './.results'


class ScalableService:
    def __init__(
        self,
        name: str,
        nodes: int,
        command: str,
        config_file: str,
        port: int,
        dockerfile: str = 'src/server/Dockerfile',
    ):
        self.name = name
        self.nodes = nodes
        self.command = command
        self.config_file = config_file
        self.port = port
        self.dockerfile = dockerfile


def create_docker_compose_base():
    return 'name: tp-escalabilidad\n'


def create_rabbitmq():
    return f"""rabbitmq:
    container_name: rabbitmq
    build:
      context: .
      dockerfile: rabbitmq/Dockerfile
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - ./rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf
    environment:
      - RABBITMQ_CONFIG_FILE=/etc/rabbitmq/rabbitmq.conf
    healthcheck:
      test: ["CMD", "rabbitmqctl", "status"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - {NETWORK_NAME}
"""


def create_client(dataset_path: str):
    return f"""
  client:
    container_name: client
    build:
      context: .
      dockerfile: src/client/Dockerfile
    command:
      [
        "python",
        "src/client/main.py",
        ".data/movies_metadata.csv",
        ".data/ratings.csv",
        ".data/credits.csv",
      ]
    image: client:latest
    environment:
      - SERVER_HOST=cleaner
      - SERVER_PORT=12345
      - BATCH_SIZE=20
    networks:
      - {NETWORK_NAME}
    volumes:
      - {dataset_path}:/app/.data
      - {RESULTS_PATH}:/app/.results
    depends_on:
      cleaner:
        condition: service_started
"""


def create_cleaner():
    return f"""cleaner:
    container_name: cleaner
    build:
      context: .
      dockerfile: src/server/Dockerfile
    command: ["python", "src/server/cleaner/main.py"]
    environment:
      - SERVER_PORT=12345
      - LISTENING_BACKLOG=3
      - BATCH_SIZE_MOVIES=20
      - BATCH_SIZE_RATINGS=100
      - BATCH_SIZE_CREDITS=20
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./src/server/cleaner/config.yaml:/app/config.yaml
"""


def create_sink(n: int):
    return f"""
  q{n}_sink:
    container_name: q{n}_sink
    build:
      context: .
      dockerfile: src/server/Dockerfile
    command: ["python", "src/server/sinks/main.py", "q{n}"]
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./src/server/sinks/q{n}_config.yaml:/app/config.yaml
"""


def create_sink_q3():
    return f"""q3_sink:
    container_name: q3_sink
    build:
      context: .
      dockerfile: src/server/Dockerfile
    command: ["python", "src/server/sinks/main.py", "q3"]
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./src/server/sinks/q3_config.yaml:/app/config.yaml
"""


def create_joiner(joiner_type: str) -> str:
    return f"""
  {joiner_type}:
    container_name: {joiner_type}
    build:
      context: .
      dockerfile: src/server/Dockerfile
    command: ["python", "src/server/joiners/main.py", {joiner_type}]
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./src/server/joiners/{joiner_type}_config.yaml:/app/config.yaml
    """


def create_node(service: ScalableService, index: int):
    container = f'{service.name}-{index}'
    peers = [
        f'{service.name}-{i}:{service.port}'
        for i in range(1, service.nodes + 1)
        if i != index
    ]
    peers_env = ','.join(peers)
    return f"""{container}:
    container_name: {container}
    build:
      context: .
      dockerfile: {service.dockerfile}
    command: {service.command}
    environment:
      - NODE_ID={container}
      - PORT={service.port}
      - PEERS={peers_env}
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - {service.config_file}:/app/config.yaml
  """


def create_scalable(service: ScalableService):
    nodes = ''
    for i in range(1, service.nodes + 1):
        nodes += create_node(service, i)

    return nodes


def create_services(scalable_services: list[ScalableService], dataset_path: str):
    rabbitmq = create_rabbitmq()
    client = create_client(dataset_path)
    cleaner = create_cleaner()
    sinks = ''
    for i in list(range(1, 6)):
        sinks += create_sink(i)
    joiners = ''
    for joiner in ['ratings', 'cast']:
        joiners += create_joiner(f'{joiner}_joiner')
    services = ''
    for service in scalable_services:
        services += create_scalable(service)

    return f"""
services:
  {rabbitmq}
  {client}
  {cleaner}
  {services}
  {sinks}
  {joiners}
"""


def create_networks():
    return f"""networks:
  {NETWORK_NAME}:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
    """


def create_docker_compose_data(
    scalable_services: list[ScalableService], dataset_path: str
):
    base = create_docker_compose_base()
    services = create_services(scalable_services, dataset_path)
    networks = create_networks()

    return base + services + networks


def parse_args():
    parser = argparse.ArgumentParser(
        prog='generate-compose', description='Docker compose generator'
    )

    parser.add_argument(
        '-s',
        '--small-dataset',
        default=False,
        action='store_true',
        help='Set if you want to use the small dataset path, else false.',
    )

    return parser.parse_args()


class Config:
    def __init__(self, data: dict):
        # Dynamically generate attributes based on the `key`s from `data`
        self.__dict__.update(data)

        if 'all' in self.__dict__ and self.all is not None and self.all > 0:
            for k in self.__dict__.keys():
                self.__dict__[k] = self.all


def read_config() -> Config:
    data = ''
    with open('nodes-config.yaml', 'r') as f:
        data = yaml.safe_load(f)

    return Config(data)


def main():
    args = parse_args()
    config = read_config()
    scalable_services = []
    base = BASE_PORT
    mapping = [
        (
            'filter_single_country',
            config.filter_single_country,
            'src/server/filters/main.py',
            'solo_country',
            './src/server/filters/single_country_config.yaml',
        ),
        (
            'country_budget_counter',
            config.country_budget_counter,
            'src/server/counters/main.py',
            'country_budget',
            './src/server/counters/country_budget_config.yaml',
        ),
        (
            'filter_budget_revenue',
            config.filter_budget_revenue,
            'src/server/filters/main.py',
            'budget_revenue',
            './src/server/filters/budget_revenue_config.yaml',
        ),
        (
            'sentiment_analyzer',
            config.sentiment_analyzer,
            'src/server/sentiment_analyzer/main.py',
            'sentiment',
            './src/server/sentiment_analyzer/config.yaml',
        ),
        (
            'filter_post_2000',
            config.filter_post_2000,
            'src/server/filters/main.py',
            'post_2000',
            './src/server/filters/post_2000_config.yaml',
        ),
        (
            'filter_argentina',
            config.filter_argentina,
            'src/server/filters/main.py',
            'argentina',
            './src/server/filters/argentina_config.yaml',
        ),
        (
            'filter_argentina_and_spain',
            config.filter_argentina_and_spain,
            'src/server/filters/main.py',
            'argentina_and_spain',
            './src/server/filters/argentina_and_spain_config.yaml',
        ),
        (
            'filter_decade_00',
            config.filter_decade_00,
            'src/server/filters/main.py',
            'decade_00',
            './src/server/filters/decade_00_config.yaml',
        ),
        (
            'rating_counter',
            config.rating_counter,
            'src/server/counters/main.py',
            'rating',
            './src/server/counters/rating_counter_config.yaml',
        ),
        (
            'cast_splitter',
            config.cast_splitter,
            'src/server/splitters/main.py',
            'cast_splitter',
            './src/server/splitters/config.yaml',
        ),
        (
            'actor_counter',
            config.actor_counter,
            'src/server/counters/main.py',
            'actor_counter',
            './src/server/counters/actor_counter_config.yaml',
        ),
    ]
    for idx, (name, count, script, logic, cfg) in enumerate(mapping):
        if count and count > 0:
            port = base + idx
            if logic:
                cmd = f'["python", "{script}", "{logic}"]'
            else:
                cmd = f'["python", "{script}"]'
            if name == 'sentiment_analyzer':
                scalable_services.append(
                    ScalableService(
                        name=name,
                        nodes=count,
                        command=cmd,
                        config_file=cfg,
                        port=port,
                        dockerfile='src/server/sentiment_analyzer/Dockerfile',
                    )
                )
            else:
                scalable_services.append(
                    ScalableService(
                        name=name,
                        nodes=count,
                        command=cmd,
                        config_file=cfg,
                        port=port,
                    )
                )

    dataset_path = SMALL_DATASET_PATH if args.small_dataset else DATASET_PATH

    content = create_docker_compose_data(scalable_services, dataset_path)

    with open(DOCKER_COMPOSE_FILENAME, 'w', encoding='utf-8') as f:
        f.write(content)


if __name__ == '__main__':
    main()

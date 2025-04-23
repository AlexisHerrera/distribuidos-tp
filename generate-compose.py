import argparse

DEFAULT_NODES = 1
DOCKER_COMPOSE_FILENAME = 'docker-compose-dev.yaml'
NETWORK_NAME = 'testing_net'
BASE_PORT = 6000


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
      test: [ "CMD", "rabbitmqctl", "status" ]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - {NETWORK_NAME}
"""


def create_client():
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
      - ./.data:/app/.data
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


def create_sink_q2():
    return f"""q2_sink:
    container_name: q2_sink
    build:
      context: .
      dockerfile: src/server/Dockerfile
    command: ["python", "src/server/sinks/main.py"]
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./src/server/sinks/config.yaml:/app/config.yaml
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
      - REPLICAS={service.nodes}
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


def create_services(scalable_services: list[ScalableService]):
    rabbitmq = create_rabbitmq()
    client = create_client()
    cleaner = create_cleaner()
    sink_q2 = create_sink_q2()
    services = ''
    for service in scalable_services:
        services += create_scalable(service)

    return f"""
services:
  {rabbitmq}
  {client}
  {cleaner}
  {services}
  {sink_q2}
"""


def create_networks():
    return f"""networks:
  {NETWORK_NAME}:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
    """


def create_docker_compose_data(scalable_services: list[ScalableService]):
    base = create_docker_compose_base()
    services = create_services(scalable_services)
    networks = create_networks()

    return base + services + networks


def parse_args():
    parser = argparse.ArgumentParser(
        prog='generate-compose',
        description='Docker compose generator',
        argument_default=DEFAULT_NODES,
    )

    parser.add_argument('--scf', '--solo-country-filter', type=int)
    parser.add_argument('--cbc', '--country-budget-counter', type=int)
    parser.add_argument('--sa', '--sentiment-analyzer', type=int)
    parser.add_argument('--p2000', '--post-2000', type=int)
    parser.add_argument('--arg', '--argentina', type=int)
    parser.add_argument('--argspa', '--argentina-and-spain', type=int)
    parser.add_argument('--d00', '--decade-00', type=int)

    return parser.parse_args()


def main():
    args = parse_args()
    scalable_services = []
    base = BASE_PORT
    mapping = [
        (
            'filter_single_country',
            args.scf,
            'src/server/filters/main.py',
            'solo_country',
            './src/server/filters/single_country_config.yaml',
        ),
        (
            'country_budget_counter',
            args.cbc,
            'src/server/counters/main.py',
            'country_budget',
            './src/server/counters/config.yaml',
        ),
        (
            'sentiment_analyzer',
            args.sa,
            'src/server/sentiment_analyzer/main.py',
            None,
            './src/server/sentiment_analyzer/config.yaml',
        ),
        (
            'filter_post_2000',
            args.p2000,
            'src/server/filters/main.py',
            'post_2000',
            './src/server/filters/post_2000_config.yaml',
        ),
        (
            'filter_argentina',
            args.arg,
            'src/server/filters/main.py',
            'argentina',
            './src/server/filters/argentina_config.yaml',
        ),
        (
            'filter_argentina_and_spain',
            args.argspa,
            'src/server/filters/main.py',
            'argentina_and_spain',
            './src/server/filters/argentina_and_spain_config.yaml',
        ),
        (
            'filter_decade_00',
            args.d00,
            'src/server/filters/main.py',
            'decade_00',
            './src/server/filters/decade_00_config.yaml',
        ),
    ]
    for idx, (name, count, script, logic, cfg) in enumerate(mapping):
        if count and count > 0:
            port = base + idx
            if logic:
                cmd = f'["python", "{script}", "{logic}"]'
            else:
                cmd = f'["python", "{script}"]'
            scalable_services.append(
                ScalableService(
                    name=name,
                    nodes=count,
                    command=cmd,
                    config_file=cfg,
                    port=port,
                )
            )
    content = create_docker_compose_data(scalable_services)
    with open(DOCKER_COMPOSE_FILENAME, 'w', encoding='utf-8') as f:
        f.write(content)


if __name__ == '__main__':
    main()

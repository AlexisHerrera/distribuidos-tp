import argparse

DEFAULT_NODES = 1
DOCKER_COMPOSE_FILENAME = 'docker-compose-dev.yaml'
NETWORK_NAME = 'testing_net'


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
      - BATCH_SIZE=20
      - RABBIT_HOST=rabbitmq
      - MOVIES_CLEANED_QUEUE=movies_cleaned_queue
      - CREDITS_CLEANED_QUEUE=credits_cleaned_queue
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
"""


def create_solo_country(n: int):
    nodes = ''
    for i in range(1, n + 1):
        node = f"""filter_single_country-{i}:
    container_name: filter_single_country-{i}
    build:
      context: .
      dockerfile: src/server/Dockerfile
    command: ["python", "src/server/filters/main.py", "solo_country"]
    environment:
      - RABBIT_HOST=rabbitmq
      - INPUT_QUEUE=movies_cleaned_queue
      - OUTPUT_QUEUE=movies_single_country_queue
      - LOG_LEVEL=INFO
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
  """
        nodes += node

    return nodes


def create_country_budget_counter(n: int):
    nodes = ''
    for i in range(1, n + 1):
        node = f"""
  country_budget_counter-{i}:
    container_name: country_budget_counter-{i}
    build:
      context: .
      dockerfile: src/server/Dockerfile
    command: ["python", "src/server/counters/main.py", "country_budget"]
    environment:
      - RABBIT_HOST=rabbitmq
      - INPUT_QUEUE=movies_single_country_queue
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
    """
        nodes += node

    return nodes


def create_sentiment_analyzer(n: int):
    nodes = ''
    for i in range(1, n + 1):
        node = f"""sentiment_analyzer-{i}:
    container_name: sentiment_analyzer-{i}
    build:
      context: .
      dockerfile: src/server/Dockerfile
    command: ["python", "src/server/sentiment_analyzer/main.py"]
    environment:
      - RABBIT_HOST=rabbitmq
    networks:
      - {NETWORK_NAME}
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./src/server/sentiment_analyzer/config.ini:/app/config.ini
  """

        nodes += node

    return nodes


def create_services(args):
    rabbitmq = create_rabbitmq()
    client = create_client()
    cleaner = create_cleaner()
    solo_country = create_solo_country(args.scf)
    country_budget_counter = create_country_budget_counter(args.cbc)
    sentiment_analyzer = create_sentiment_analyzer(args.sa)
    return f"""
services:
  {rabbitmq}
  {client}
  {cleaner}
  {solo_country}
  {country_budget_counter}
  {sentiment_analyzer}
"""


def create_networks():
    return f"""networks:
  {NETWORK_NAME}:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
    """


def create_docker_compose_data(args):
    base = create_docker_compose_base()
    services = create_services(args)
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

    return parser.parse_args()


def main():
    args = parse_args()

    content = create_docker_compose_data(args)

    with open(DOCKER_COMPOSE_FILENAME, 'w', encoding='utf-8') as f:
        f.writelines(content)


if __name__ == '__main__':
    main()

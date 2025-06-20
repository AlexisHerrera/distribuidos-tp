import yaml


class IndentDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def read_num_clients(scaling_config_path: str) -> int:
    try:
        with open(scaling_config_path, 'r') as f:
            data = yaml.safe_load(f)
            num_clients = data.get('clients', None)
            if not isinstance(num_clients, int) or num_clients < 1:
                raise ValueError("Invalid or missing 'clients' key in scaling config.")
            return num_clients
    except FileNotFoundError:
        print(f"Scaling config file not found at: {scaling_config_path}")
        raise
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {e}")
        raise


def create_config(num_clients: int) -> dict:
    config = {
        'rabbit': {
            'host': 'rabbitmq'
        },
        'connection': {
            'consumer': [
                {
                    'type': 'direct',
                    'queue': 'reporter'
                }
            ],
            'publisher': [
                {
                    'type': 'broadcast',
                    'exchange': 'movies_metadata_clean',
                    'msg_type': 'Movie'
                },
                {
                    'type': 'direct',
                    'queue': 'ratings_clean',
                    'msg_type': 'Rating'
                },
                {
                    'type': 'direct',
                    'queue': 'credits_clean',
                    'msg_type': 'Cast'
                }
            ]
        },
        'heartbeat': {
            'port': 13434
        },
        'log': {
            'level': 'INFO'
        }
    }

    for i in range(1, num_clients + 1):
        config['connection']['publisher'].append({
            'type': 'direct',
            'queue': f'ratings_clean_{i}',
            'msg_type': 'Rating'
        })
        config['connection']['publisher'].append({
            'type': 'direct',
            'queue': f'credits_clean_{i}',
            'msg_type': 'Cast'
        })

    return config


def main():
    scaling_config_path = 'nodes-config.yaml'
    output_config_path = 'src/server/cleaner/config.yaml'

    try:
        num_clients = read_num_clients(scaling_config_path)
        config = create_config(num_clients)

        with open(output_config_path, 'w') as file:
            yaml.dump(config, file, Dumper=IndentDumper, default_flow_style=False, sort_keys=False)

        print(f"Configuration file '{output_config_path}' created successfully with {num_clients} clients.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()

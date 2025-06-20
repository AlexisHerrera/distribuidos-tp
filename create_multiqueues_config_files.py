import yaml
import csv


class IndentDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def read_uuids_from_csv(csv_path: str) -> list[str]:
    try:
        with open(csv_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            row = next(reader)
            if not row:
                raise ValueError("CSV file is empty or improperly formatted.")
            return row
    except FileNotFoundError:
        print(f"UUID CSV file not found at: {csv_path}")
        raise
    except Exception as e:
        print(f"Error reading UUID CSV file: {e}")
        raise


def create_config(client_uuids: list[str]) -> dict:
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

    for client_id in client_uuids:
        config['connection']['publisher'].append({
            'type': 'direct',
            'queue': f'ratings_clean_{client_id}',
            'msg_type': 'Rating'
        })
        config['connection']['publisher'].append({
            'type': 'direct',
            'queue': f'credits_clean_{client_id}',
            'msg_type': 'Cast'
        })

    return config


def main():
    uuid_csv_path = 'client_uuids.csv'
    output_config_path = 'src/server/cleaner/config.yaml'

    try:
        client_uuids = read_uuids_from_csv(uuid_csv_path)
        config = create_config(client_uuids)

        with open(output_config_path, 'w') as file:
            yaml.dump(config, file, Dumper=IndentDumper, default_flow_style=False, sort_keys=False)

        print(f"Configuration file '{output_config_path}' created successfully with {len(client_uuids)} UUID-based queues.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()
